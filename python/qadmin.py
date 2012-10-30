#! /usr/bin/env python

"""Commands that require only database connection:

    connect dbname=.. host=.. service=.. queue=..;
    connect [ queue=.. ] [ node=.. ];
    install pgq | londiste;

    show queue [ <qname | *> ];
    create queue <qname>;
    alter queue <qname | *> set param = , ...;
    drop queue <qname>;

    show consumer [ <cname | *> [on <qname>] ];
    register consumer <consumer> [on <qname> | at <tick_id> | copy <consumer> ]* ;
    unregister consumer <consumer | *> [from <qname>];
    register subconsumer <subconsumer> for <consumer> [on <qname>];
    unregister subconsumer <subconsumer | *> for <consumer> [from <qname>] [close [batch]];

    show node [ <node | *> [on <qname>] ];
    show table <tbl>;
    show sequence <seq>;

Following commands expect default queue:

    show batch <batch_id>;
    show batch <consumer>;

Londiste commands:

    londiste add table <tbl> [ , ... ]
        with skip_truncate, tgflags='UIDBAQL',
             expect_sync, no_triggers,
             -- pass trigger args:
             backup, skip, when='EXPR', ev_XX='EXPR';
    londiste add sequence <seq>;
    londiste remove table <tbl> [ , ... ];
    londiste remove sequence <seq> [ , ... ];
    londiste tables;
    londiste seqs;
    londiste missing;

Other commands:

    exit;  - quit program
    ^D     - quit program
    ^C     - clear current buffer
"""

# unimplemented:
"""
create <root | branch | leaf> node <node> location <loc> [on <qname>];
drop node <node> [on <qname>];
alter node <node> [location=<loc>]
show_queue_stats <q>;

change provider
drop node
status
rename node

node create

create root_node <name>;
create branch_node <name>;
create leaf_node <name>;

alter node <name> provider <new>;

alter node <name> takeover <oldnow> with all;
alter node <name> rename <new>;

takeover <oldnode>;

drop node <name>;

show node [ <node | *> [on <qname>] ];
show cascade;

"""

cmdline_usage = '''\
Usage: qadmin [switches]

Initial connection options:
    -h host
    -p port
    -U user
    -d dbname
    -Q queuename

Command options:
    -c cmd_string
    -f execfile

General options:
    --help
    --version
'''

import sys, os, readline, getopt, re, psycopg2, traceback

import pkgloader
pkgloader.require('skytools', '3.0')
import skytools

__version__ = skytools.__version__

script = None

IGNORE_HOSTS = {
    'ip6-allhosts': 1,
    'ip6-allnodes': 1,
    'ip6-allrouters': 1,
    #'ip6-localhost': 1,
    'ip6-localnet': 1,
    'ip6-loopback': 1,
    'ip6-mcastprefix': 1,
}

_ident_rx =''' ( " ( "" | [^"]+ )* " ) | ( [a-z_][a-z0-9_]* ) | [.] | (?P<err> .)  '''
_ident_rc = re.compile(_ident_rx, re.X | re.I)

def unquote_any(typ, s):
    global _ident_rc
    if typ == 'ident':
        res = []
        pos = 0
        while 1:
            m = _ident_rc.match(s, pos)
            if not m:
                break
            if m.group('err'):
                raise Exception('invalid syntax for ident')
            s1 = m.group()
            if s1[0] == '"':
                s1 = s1[1:-1].replace('""', '"')
            res.append(s1)
            pos = m.end()
        s = ''.join(res)
    elif typ == 'str' or typ == 'dolq':
        s = skytools.unquote_literal(s, True)
    return s

def normalize_any(typ, s):
    if typ == 'ident' and s.find('"') < 0:
        s = s.lower()
    return s

def display_result(curs, desc, fields = []):
    """Display multirow query as a table."""

    rows = curs.fetchall()

    if not fields:
        fields = [f[0] for f in curs.description]

    widths = [10] * len(fields)
    for i, f in enumerate(fields):
        rlen = len(f)
        if rlen > widths[i]:
            widths[i] = rlen
    for row in rows:
        for i, k in enumerate(fields):
            rlen = row[k] and len(str(row[k])) or 0
            if rlen > widths[i]:
                widths[i] = rlen
    widths = [w + 2 for w in widths]

    fmt = '%%-%ds' * (len(widths) - 1) + '%%s'
    fmt = fmt % tuple(widths[:-1])
    if desc:
        print(desc)
    print(fmt % tuple(fields))
    print(fmt % tuple([ '-' * (w - 2) for w in widths ]))

    for row in rows:
        print(fmt % tuple([row[k] for k in fields]))
    print('')

##
## Base token classes
##

class Token:
    """Base class for tokens.

    The optional 'param' kwarg will set corresponding key in
    'params' dict to final token value.
    """
    # string to append to completions
    c_append = ' '

    # token type to accept
    tk_type = ("ident", "dolq", "str", "num", "sym")
    # skipped: numarg, pyold, pynew

    def __init__(self, next = None, name = None, append = 0):
        self.next = next
        self.name = name
        self._append = append

    # top-level api

    def get_next(self, typ, word, params):
        """Return next token if 'word' matches this token."""
        if not self.is_acceptable(typ, word):
            return None
        self.set_param(typ, word, params)
        return self.next

    def get_completions(self, params):
        """Return list of all completions possible at this point."""
        wlist = self.get_wlist()
        comp_list = [w + self.c_append for w in wlist]
        return comp_list

    # internal api

    def get_wlist(self):
        """Return list of potential words at this point."""
        return []

    def set_param(self, typ, word, params):
        # now set special param
        if not self.name:
            return
        uw = unquote_any(typ, word)
        if self._append:
            lst = params.setdefault(self.name, [])
            lst.append(uw)
        else:
            params[self.name] = uw

    def is_acceptable(self, tok, word):
        if tok not in self.tk_type:
            return False
        return True

class Exact(Token):
    """Single fixed token."""
    def __init__(self, value, next, **kwargs):
        Token.__init__(self, next, **kwargs)
        self.value = value
    def get_wlist(self):
        return [self.value]
    def is_acceptable(self, typ, word):
        if not Token.is_acceptable(self, typ, word):
            return False
        return word == self.value

class List(Token):
    """List of Tokens, will be tried sequentially until one matches."""
    def __init__(self, *args, **kwargs):
        Token.__init__(self, **kwargs)
        self.tok_list = list(args)

    def add(self, *args):
        for a in args:
            self.tok_list.append(a)

    def get_next(self, typ, word, params):
        for w in self.tok_list:
            n = w.get_next(typ, word, params)
            if n:
                self.set_param(typ, word, params)
                return n
        return None

    def get_completions(self, params):
        comp_list = []
        for w in self.tok_list:
            comp_list += w.get_completions(params)
        return comp_list

##
## Dynamic token classes
##

class ConnstrPassword(Token):
    tk_type = ("str", "num", "ident")

class StrValue(Token):
    tk_type = ("str",)

class NumValue(Token):
    tk_type = ("num",)

class Word(Exact):
    """Single fixed keyword."""
    tk_type = ("ident",)

class Name(Token):
    """Dynamically generated list of idents."""
    tk_type = ("ident")

class Symbol(Exact):
    """Single fixed symbol."""
    tk_type = ("sym",)
    c_append = ''

class XSymbol(Symbol):
    """Symbol that is not shown in completion."""
    def get_wlist(self):
        return []

class SubConsumerName(Token):
    tk_type = ("str", "num", "ident")

# data-dependant completions

class Queue(Name):
    def get_wlist(self):
        return script.get_queue_list()

class Consumer(Name):
    def get_wlist(self):
        return script.get_consumer_list()

class DBNode(Name):
    def get_wlist(self):
        return script.get_node_list()

class Database(Name):
    def get_wlist(self):
        return script.get_database_list()

class Host(Name):
    def get_wlist(self):
        return script.get_host_list()

class User(Name):
    def get_wlist(self):
        return script.get_user_list()

class NewTable(Name):
    def get_wlist(self):
        return script.get_new_table_list()

class KnownTable(Name):
    def get_wlist(self):
        return script.get_known_table_list()

class PlainTable(Name):
    def get_wlist(self):
        return script.get_plain_table_list()

class PlainSequence(Name):
    def get_wlist(self):
        return script.get_plain_seq_list()

class NewSeq(Name):
    def get_wlist(self):
        return script.get_new_seq_list()

class KnownSeq(Name):
    def get_wlist(self):
        return script.get_known_seq_list()

class BatchId(NumValue):
    def get_wlist(self):
        return script.get_batch_list()

class TickId(NumValue):
    def get_wlist(self):
        return []

class Port(NumValue):
    def get_wlist(self):
        return ['5432', '6432']

# easier completion - add follow-up symbols

class WordEQ(Word):
    """Word that is followed by '='."""
    c_append = '='
    def __init__(self, word, next, **kwargs):
        next = Symbol('=', next)
        Word.__init__(self, word, next, **kwargs)

class WordEQQ(Word):
    """Word that is followed by '=' and string."""
    c_append = "='"
    def __init__(self, word, next, **kwargs):
        next = Symbol('=', next)
        Word.__init__(self, word, next, **kwargs)

##
##  Now describe the syntax.
##

top_level = List(name = 'cmd')

w_done = Symbol(';', top_level)
w_xdone = XSymbol(';', top_level)

w_sql = List(w_done)
w_sql.add(Token(w_sql))

w_connect = List()
w_connect.add(
        WordEQ('dbname', Database(w_connect, name = 'dbname')),
        WordEQ('host', Host(w_connect, name = 'host')),
        WordEQ('port', Port(w_connect, name = 'port')),
        WordEQ('user', User(w_connect, name = 'user')),
        WordEQ('password', ConnstrPassword(w_connect, name = 'password')),
        WordEQ('queue', Queue(w_connect, name = 'queue')),
        WordEQ('node', DBNode(w_connect, name = 'node')),
        w_done)

w_show_batch = List(
    BatchId(w_done, name = 'batch_id'),
    Consumer(w_done, name = 'consumer'))

w_show_queue = List(
    Symbol('*', w_done, name = 'queue'),
    Queue(w_done, name = 'queue'),
    w_done)

w_show_on_queue = List(
    Symbol('*', w_done, name = 'queue'),
    Queue(w_done, name = 'queue'),
    )

w_on_queue = List(Word('on', w_show_on_queue), w_done)

w_show_consumer = List(
    Symbol('*', w_on_queue, name = 'consumer'),
    Consumer(w_on_queue, name = 'consumer'),
    w_done)

w_show_node = List(
    Symbol('*', w_on_queue, name = 'node'),
    DBNode(w_on_queue, name = 'node'),
    w_done)

w_show_table = PlainTable(w_done, name = 'table')

w_show_seq = PlainSequence(w_done, name = 'seq')

w_show = List(
    Word('batch', w_show_batch),
    Word('help', w_done),
    Word('queue', w_show_queue),
    Word('consumer', w_show_consumer),
    Word('node', w_show_node),
    Word('table', w_show_table),
    Word('sequence', w_show_seq),
    Word('version', w_done),
    name = "cmd2")

w_install = List(
    Word('pgq', w_done),
    Word('londiste', w_done),
    name = 'module')

# alter queue
w_qargs2 = List()

w_qargs = List(
    WordEQQ('idle_period', StrValue(w_qargs2, name = 'ticker_idle_period')),
    WordEQ('max_count', NumValue(w_qargs2, name = 'ticker_max_count')),
    WordEQQ('max_lag', StrValue(w_qargs2, name = 'ticker_max_lag')),
    WordEQ('paused', NumValue(w_qargs2, name = 'ticker_paused')))

w_qargs2.add(w_done)
w_qargs2.add(Symbol(',', w_qargs))

w_set_q = Word('set', w_qargs)

w_alter_q = List(
        Symbol('*', w_set_q, name = 'queue'),
        Queue(w_set_q, name = 'queue'))

# alter
w_alter = List(
        Word('queue', w_alter_q),
        w_sql,
        name = 'cmd2')

# create
w_create = List(
        Word('queue', Queue(w_done, name = 'queue')),
        w_sql,
        name = 'cmd2')

# drop
w_drop = List(
        Word('queue', Queue(w_done, name = 'queue')),
        w_sql,
        name = 'cmd2')

# register
w_reg_target = List()
w_reg_target.add(
        Word('on', Queue(w_reg_target, name = 'queue')),
        Word('copy', Consumer(w_reg_target, name = 'copy_reg')),
        Word('at', TickId(w_reg_target, name = 'at_tick')),
        w_done)

w_cons_on_queue = Word('consumer',
        Consumer(w_reg_target, name = 'consumer'),
        name = 'cmd2')

w_sub_reg_target = List()
w_sub_reg_target.add(
        Word('on', Queue(w_sub_reg_target, name = 'queue')),
        Word('for', Consumer(w_sub_reg_target, name = 'consumer')),
        w_done)

w_subcons_on_queue = Word('subconsumer',
        SubConsumerName(w_sub_reg_target, name = 'subconsumer'),
        name = 'cmd2')

w_register = List(w_cons_on_queue,
                  w_subcons_on_queue)

# unregister

w_from_queue = List(w_done, Word('from', Queue(w_done, name = 'queue')))
w_cons_from_queue = Word('consumer',
        List( Symbol('*', w_from_queue, name = 'consumer'),
              Consumer(w_from_queue, name = 'consumer')
            ),
        name = 'cmd2')

w_done_close = List(w_done,
            Word('close', List(w_done, Word('batch', w_done)), name = 'close'))
w_from_queue_close = List(w_done_close,
                          Word('from', Queue(w_done_close, name = 'queue')))
w_con_from_queue = Consumer(w_from_queue_close, name = 'consumer')
w_subcons_from_queue = Word('subconsumer',
    List( Symbol('*', Word('for', w_con_from_queue), name = 'subconsumer'),
          SubConsumerName(Word('for', w_con_from_queue), name = 'subconsumer')
        ),
    name = 'cmd2')

w_unregister = List(w_cons_from_queue,
                    w_subcons_from_queue)

# londiste add table
w_table_with2 = List()
w_table_with = List(
    Word('skip_truncate', w_table_with2, name = 'skip_truncate'),
    Word('expect_sync', w_table_with2, name = 'expect_sync'),
    Word('backup', w_table_with2, name = 'backup'),
    Word('skip', w_table_with2, name = 'skip'),
    Word('no_triggers', w_table_with2, name = 'no_triggers'),
    WordEQQ('ev_ignore', StrValue(w_table_with2, name = 'ignore')),
    WordEQQ('ev_type', StrValue(w_table_with2, name = 'ev_type')),
    WordEQQ('ev_data', StrValue(w_table_with2, name = 'ev_data')),
    WordEQQ('ev_extra1', StrValue(w_table_with2, name = 'ev_extra1')),
    WordEQQ('ev_extra2', StrValue(w_table_with2, name = 'ev_extra2')),
    WordEQQ('ev_extra3', StrValue(w_table_with2, name = 'ev_extra3')),
    WordEQQ('ev_extra4', StrValue(w_table_with2, name = 'ev_extra4')),
    WordEQQ('pkey', StrValue(w_table_with2, name = 'pkey')),
    WordEQQ('when', StrValue(w_table_with2, name = 'when')),
    WordEQQ('tgflags', StrValue(w_table_with2, name = 'tgflags'))
    )

w_table_with2.add(w_done)
w_table_with2.add(Symbol(',', w_table_with))

w_londiste_add_table = List()
w_londiste_add_table2 = List(
    Symbol(',', w_londiste_add_table),
    Word('with', w_table_with),
    w_done)
w_londiste_add_table.add(
    NewTable(w_londiste_add_table2,
             name = 'tables', append = 1))

# londiste add seq
w_londiste_add_seq = List()
w_londiste_add_seq2 = List(
    Symbol(',', w_londiste_add_seq),
    w_done)
w_londiste_add_seq.add(
    NewSeq(w_londiste_add_seq2, name = 'seqs', append = 1))

# londiste remove table
w_londiste_remove_table = List()
w_londiste_remove_table2 = List(
    Symbol(',', w_londiste_remove_table),
    w_done)
w_londiste_remove_table.add(
    KnownTable(w_londiste_remove_table2, name = 'tables', append = 1))

# londiste remove sequence
w_londiste_remove_seq = List()
w_londiste_remove_seq2 = List(
    Symbol(',', w_londiste_remove_seq),
    w_done)
w_londiste_remove_seq.add(
    KnownSeq(w_londiste_remove_seq2, name = 'seqs', append = 1))

w_londiste_add = List(
        Word('table', w_londiste_add_table),
        Word('sequence', w_londiste_add_seq),
        name = 'cmd3')

w_londiste_remove = List(
        Word('table', w_londiste_remove_table),
        Word('sequence', w_londiste_remove_seq),
        name = 'cmd3')

# londiste
w_londiste = List(
    Word('add', w_londiste_add),
    Word('remove', w_londiste_remove),
    Word('missing', w_done),
    Word('tables', w_done),
    Word('seqs', w_done),
    name = "cmd2")

top_level.add(
    Word('alter', w_alter),
    Word('connect', w_connect),
    Word('create', w_create),
    Word('drop', w_drop),
    Word('install', w_install),
    Word('register', w_register),
    Word('unregister', w_unregister),
    Word('show', w_show),
    Word('exit', w_done),
    Word('londiste', w_londiste),

    Word('select', w_sql),
    w_sql)

##
## Main class for keeping the state.
##

class AdminConsole:
    cur_queue = None
    cur_database = None

    server_version = None
    pgq_version = None

    cmd_file = None
    cmd_str = None

    comp_cache = {
        'comp_pfx': None,
        'comp_list': None,
        'queue_list': None,
        'database_list': None,
        'consumer_list': None,
        'host_list': None,
        'user_list': None,
    }
    db = None
    initial_connstr = None

    rc_hosts = re.compile('\s+')

    def get_queue_list(self):
        q = "select queue_name from pgq.queue order by 1"
        return self._ccache('queue_list', q, 'pgq')

    def get_database_list(self):
        q = "select datname from pg_catalog.pg_database order by 1"
        return self._ccache('database_list', q)

    def get_user_list(self):
        q = "select usename from pg_catalog.pg_user order by 1"
        return self._ccache('user_list', q)

    def get_consumer_list(self):
        q = "select co_name from pgq.consumer order by 1"
        return self._ccache('consumer_list', q, 'pgq')

    def get_node_list(self):
        q = "select distinct node_name from pgq_node.node_location order by 1"
        return self._ccache('node_list', q, 'pgq_node')

    def _new_obj_sql(self, queue, objname, objkind):
        args = {'queue': skytools.quote_literal(queue),
                'obj': objname,
                'ifield': objname + '_name',
                'itable': 'londiste.' + objname + '_info',
                'kind': skytools.quote_literal(objkind),
            }
        q = """select quote_ident(n.nspname) || '.' || quote_ident(r.relname)
            from pg_catalog.pg_class r
            join pg_catalog.pg_namespace n on (n.oid = r.relnamespace)
            left join %(itable)s i
                 on (i.queue_name = %(queue)s and
                     i.%(ifield)s = (n.nspname || '.' || r.relname))
            where r.relkind = %(kind)s
              and n.nspname not in ('pg_catalog', 'information_schema', 'pgq', 'londiste', 'pgq_node', 'pgq_ext')
              and n.nspname !~ 'pg_.*'
              and i.%(ifield)s is null
            union all
            select londiste.quote_fqname(%(ifield)s) from %(itable)s
             where queue_name = %(queue)s and not local
            order by 1 """ % args
        return q

    def get_new_table_list(self):
        if not self.cur_queue:
            return []
        q = self._new_obj_sql(self.cur_queue, 'table', 'r')
        return self._ccache('new_table_list', q, 'londiste')

    def get_new_seq_list(self):
        if not self.cur_queue:
            return []
        q = self._new_obj_sql(self.cur_queue, 'seq', 'S')
        return self._ccache('new_seq_list', q, 'londiste')

    def get_known_table_list(self):
        if not self.cur_queue:
            return []
        qname = skytools.quote_literal(self.cur_queue)
        q = "select londiste.quote_fqname(table_name)"\
            " from londiste.table_info"\
            " where queue_name = %s order by 1" % qname
        return self._ccache('known_table_list', q, 'londiste')

    def get_known_seq_list(self):
        if not self.cur_queue:
            return []
        qname = skytools.quote_literal(self.cur_queue)
        q = "select londiste.quote_fqname(seq_name)"\
            " from londiste.seq_info"\
            " where queue_name = %s order by 1" % qname
        return self._ccache('known_seq_list', q, 'londiste')

    def get_plain_table_list(self):
        q = "select quote_ident(n.nspname) || '.' || quote_ident(r.relname)"\
            " from pg_class r join pg_namespace n on (n.oid = r.relnamespace)"\
            " where r.relkind = 'r' "\
            "   and n.nspname not in ('pg_catalog', 'information_schema', 'pgq', 'londiste', 'pgq_node', 'pgq_ext') "\
            "   and n.nspname !~ 'pg_.*' "\
            " order by 1"
        return self._ccache('plain_table_list', q)

    def get_plain_seq_list(self):
        q = "select quote_ident(n.nspname) || '.' || quote_ident(r.relname)"\
            " from pg_class r join pg_namespace n on (n.oid = r.relnamespace)"\
            " where r.relkind = 'S' "\
            "   and n.nspname not in ('pg_catalog', 'information_schema', 'pgq', 'londiste', 'pgq_node', 'pgq_ext') "\
            " order by 1"
        return self._ccache('plain_seq_list', q)

    def get_batch_list(self):
        if not self.cur_queue:
            return []
        qname = skytools.quote_literal(self.cur_queue)
        q = "select current_batch::text from pgq.get_consumer_info(%s)"\
            " where current_batch is not null order by 1" % qname
        return self._ccache('batch_list', q, 'pgq')

    def _ccache(self, cname, q, req_schema = None):
        if not self.db:
            return []

        # check if schema exists
        if req_schema:
            k = "schema_exists_%s" % req_schema
            ok = self.comp_cache.get(k)
            if ok is None:
                curs = self.db.cursor()
                ok = skytools.exists_schema(curs, req_schema)
                self.comp_cache[k] = ok
            if not ok:
                return []

        # actual completion
        clist = self.comp_cache.get(cname)
        if clist is None:
            curs = self.db.cursor()
            curs.execute(q)
            clist = [r[0] for r in curs.fetchall()]
            self.comp_cache[cname] = clist
        return clist

    def get_host_list(self):
        clist = self.comp_cache.get('host_list')
        if clist is None:
            try:
                f = open('/etc/hosts', 'r')
                clist = []
                while 1:
                    ln = f.readline()
                    if not ln:
                        break
                    ln = ln.strip()
                    if ln == '' or ln[0] == '#':
                        continue
                    lst = self.rc_hosts.split(ln)
                    for h in lst[1:]:
                        if h not in IGNORE_HOSTS:
                            clist.append(h)
                clist.sort()
                self.comp_cache['host_list'] = clist
            except IOError:
                clist = []
        return clist

    def parse_cmdline(self, argv):
        switches = "c:h:p:d:U:f:Q:"
        lswitches = ['help', 'version']
        try:
            opts, args = getopt.getopt(argv, switches, lswitches)
        except getopt.GetoptError, ex:
            print str(ex)
            print "Use --help to see command line options"
            sys.exit(1)

        cstr_map = {
            'dbname': None,
            'host': None,
            'port': None,
            'user': None,
            'password': None,
        }
        cmd_file = cmd_str = None
        for o, a in opts:
            if o == "--help":
                print cmdline_usage
                sys.exit(0)
            elif o == "--version":
                print "qadmin version %s" % __version__
                sys.exit(0)
            elif o == "-h":
                cstr_map['host'] = a
            elif o == "-p":
                cstr_map['port'] = a
            elif o == "-d":
                cstr_map['dbname'] = a
            elif o == "-U":
                cstr_map['user'] = a
            elif o == "-Q":
                self.cur_queue = a
            elif o == "-c":
                self.cmd_str = a
            elif o == "-f":
                self.cmd_file = a

        cstr_list = []
        for k, v in cstr_map.items():
            if v is not None:
                cstr_list.append("%s=%s" % (k, v))
        if len(args) == 1:
            a = args[0]
            if a.find('=') >= 0:
                cstr_list.append(a)
            else:
                cstr_list.append("dbname=%s" % a)
        elif len(args) > 1:
            print "too many arguments, use --help to see syntax"
            sys.exit(1)

        self.initial_connstr = " ".join(cstr_list)

    def db_connect(self, connstr, quiet=False):
        db = skytools.connect_database(connstr)
        db.set_isolation_level(0) # autocommit

        q = "select current_database(), current_setting('server_version')"
        curs = db.cursor()
        curs.execute(q)
        res = curs.fetchone()
        self.cur_database = res[0]
        self.server_version = res[1]
        q = "select pgq.version()"
        try:
            curs.execute(q)
            res = curs.fetchone()
            self.pgq_version = res[0]
        except psycopg2.ProgrammingError:
            self.pgq_version = "<none>"
        if not quiet:
            print "qadmin (%s, server %s, pgq %s)" % (__version__, self.server_version, self.pgq_version)
            #print "Connected to %r" % connstr
        return db

    def run(self, argv):
        self.parse_cmdline(argv)

        if self.cmd_file is not None and self.cmd_str is not None:
            print "cannot handle -c and -f together"
            sys.exit(1)

        # append ; to cmd_str if needed
        if self.cmd_str and not self.cmd_str.rstrip().endswith(';'):
            self.cmd_str += ';'

        cmd_str = self.cmd_str
        if self.cmd_file:
            cmd_str = open(self.cmd_file, "r").read()

        try:
            self.db = self.db_connect(self.initial_connstr, quiet=True)
        except psycopg2.Error, d:
            print str(d).strip()
            sys.exit(1)

        if cmd_str:
            self.exec_string(cmd_str)
        else:
            self.main_loop()

    def main_loop(self):
        readline.parse_and_bind('tab: complete')
        readline.set_completer(self.rl_completer_safe)
        #print 'delims: ', repr(readline.get_completer_delims())
        # remove " from delims
        #readline.set_completer_delims(" \t\n`~!@#$%^&*()-=+[{]}\\|;:',<>/?")

        hist_file = os.path.expanduser("~/.qadmin_history")
        try:
            readline.read_history_file(hist_file)
        except IOError:
            pass

        print "Welcome to qadmin %s (server %s), the PgQ interactive terminal." % (__version__, self.server_version)
        print "Use 'show help;' to see available commands."
        while 1:
            try:
                ln = self.line_input()
                self.exec_string(ln)
            except KeyboardInterrupt:
                print
            except EOFError:
                print
                break
            except psycopg2.Error, d:
                print 'ERROR:', str(d).strip()
            except Exception:
                traceback.print_exc()
            self.reset_comp_cache()

        try:
            readline.write_history_file(hist_file)
        except IOError:
            pass

    def rl_completer(self, curword, state):
        curline = readline.get_line_buffer()
        start = readline.get_begidx()
        end = readline.get_endidx()

        pfx = curline[:start]
        sglist = self.find_suggestions(pfx, curword)
        if state < len(sglist):
            return sglist[state]
        return None

    def rl_completer_safe(self, curword, state):
        try:
            return self.rl_completer(curword, state)
        except BaseException, det:
            print 'got some error', str(det)

    def line_input(self):
        qname = "(noqueue)"
        if self.cur_queue:
            qname = self.cur_queue
        p = "%s@%s> " % (qname, self.cur_database)
        return raw_input(p)

    def sql_words(self, sql):
        return skytools.sql_tokenizer(sql,
                standard_quoting = True,
                fqident = True,
                show_location = True,
                ignore_whitespace = True)

    def reset_comp_cache(self):
        self.comp_cache = {}

    def find_suggestions(self, pfx, curword, params = {}):

        # refresh word cache
        c_pfx = self.comp_cache.get('comp_pfx')
        c_list = self.comp_cache.get('comp_list', [])
        c_pos = self.comp_cache.get('comp_pos')
        if c_pfx != pfx:
            c_list, c_pos = self.find_suggestions_real(pfx, params)
            orig_pos = c_pos
            while c_pos < len(pfx) and pfx[c_pos].isspace():
                c_pos += 1
            #print repr(pfx), orig_pos, c_pos
            self.comp_cache['comp_pfx'] = pfx
            self.comp_cache['comp_list'] = c_list
            self.comp_cache['comp_pos'] = c_pos

        skip = len(pfx) - c_pos
        if skip:
            curword = pfx[c_pos : ] + curword

        # generate suggestions
        wlen = len(curword)
        res = []
        for cword in c_list:
            if curword == cword[:wlen]:
                res.append(cword)

        # resync with readline offset
        if skip:
            res = [s[skip:] for s in res]
        #print '\nfind_suggestions', repr(pfx), repr(curword), repr(res), repr(c_list)
        return res

    def find_suggestions_real(self, pfx, params):
        # find level
        node = top_level
        pos = 0
        xpos = 0
        xnode = node
        for typ, w, pos in self.sql_words(pfx):
            w = normalize_any(typ, w)
            node = node.get_next(typ, w, params)
            if not node:
                break
            xnode = node
            xpos = pos

        # find possible matches
        if xnode:
            return (xnode.get_completions(params), xpos)
        else:
            return ([], xpos)

    def exec_string(self, ln, eof = False):
        node = top_level
        params = {}
        self.tokens = []
        for typ, w, pos in self.sql_words(ln):
            self.tokens.append((typ, w))
            w = normalize_any(typ, w)
            if typ == 'error':
                print 'syntax error 1:', repr(ln)
                return
            onode = node
            node = node.get_next(typ, w, params)
            if not node:
                print "syntax error 2:", repr(ln), repr(typ), repr(w), repr(params)
                return
            if node == top_level:
                self.exec_params(params)
                params = {}
                self.tokens = []
        if eof:
            if params:
                self.exec_params(params)
        elif node != top_level:
            print "multi-line commands not supported:", repr(ln)

    def exec_params(self, params):
        #print 'RUN', params
        cmd = params.get('cmd')
        cmd2 = params.get('cmd2')
        cmd3 = params.get('cmd3')
        if not cmd:
            print 'parse error: no command found'
            return
        if cmd2:
            cmd = "%s_%s" % (cmd, cmd2)
        if cmd3:
            cmd = "%s_%s" % (cmd, cmd3)
        #print 'RUN', repr(params)
        fn = getattr(self, 'cmd_' + cmd, self.execute_sql)
        fn(params)

    def cmd_connect(self, params):
        qname = params.get('queue', self.cur_queue)

        if 'node' in params and not qname:
            print 'node= needs a queue also'
            return

        # load raw connection params
        cdata = []
        for k in ('dbname', 'host', 'port', 'user', 'password'):
            if k in params:
                arg = "%s=%s" % (k, params[k])
                cdata.append(arg)

        # raw connect
        if cdata:
            if 'node' in params:
                print 'node= cannot be used together with raw params'
                return
            cstr = " ".join(cdata)
            self.db = self.db_connect(cstr)

        # connect to queue
        if qname:
            curs = self.db.cursor()
            q = "select queue_name from pgq.get_queue_info(%s)"
            curs.execute(q, [qname])
            res = curs.fetchall()
            if len(res) == 0:
                print 'queue not found'
                return

            if 'node' in params:
                q = "select node_location from pgq_node.get_queue_locations(%s)"\
                    " where node_name = %s"
                curs.execute(q, [qname, params['node']])
                res = curs.fetchall()
                if len(res) == 0:
                    print "node not found"
                    return
                cstr = res[0]['node_location']
                self.db = self.db_connect(cstr)

        # set default queue
        if 'queue' in params:
            self.cur_queue = qname

        print "CONNECT"

    def cmd_show_version (self, params):
        print "qadmin version %s" % __version__
        print "server version %s" % self.server_version
        print "pgq version %s" % self.pgq_version

    def cmd_install(self, params):
        pgq_objs = [
            skytools.DBLanguage("plpgsql"),
            #skytools.DBFunction("txid_current_snapshot", 0, sql_file="txid.sql"),
            skytools.DBSchema("pgq", sql_file="pgq.sql"),
            skytools.DBSchema("pgq_ext", sql_file="pgq_ext.sql"),
            skytools.DBSchema("pgq_node", sql_file="pgq_node.sql"),
            skytools.DBSchema("pgq_coop", sql_file="pgq_coop.sql"),
        ]
        londiste_objs = pgq_objs + [
            skytools.DBSchema("londiste", sql_file="londiste.sql"),
        ]
        mod_map = {
            'londiste': londiste_objs,
            'pgq': pgq_objs,
        }
        mod_name = params['module']
        objs = mod_map[mod_name]
        if not self.db:
            print "no db?"
            return
        curs = self.db.cursor()
        skytools.db_install(curs, objs, None)
        print "INSTALL"

    def cmd_show_queue(self, params):
        queue = params.get('queue')
        if queue is None:
            # "show queue" without args, show all if not connected to
            # specific queue
            queue = self.cur_queue
            if not queue:
                queue = '*'
        curs = self.db.cursor()
        fields = [
            "queue_name",
            "queue_cur_table || '/' || queue_ntables as tables",
            "queue_ticker_max_count as max_count",
            "queue_ticker_max_lag as max_lag",
            "queue_ticker_idle_period as idle_period",
            "queue_ticker_paused as paused",
            "ticker_lag",
            "ev_per_sec",
            "ev_new",
        ]
        pfx = "select " + ",".join(fields)

        if queue == '*':
            q = pfx + " from pgq.get_queue_info()"
            curs.execute(q)
        else:
            q = pfx + " from pgq.get_queue_info(%s)"
            curs.execute(q, [queue])

        display_result(curs, 'Queue "%s":' % queue)

    def cmd_show_consumer(self, params):
        """Show consumer status"""
        consumer = params.get('consumer', '*')
        queue = params.get('queue', '*')

        q_queue = (queue != '*' and queue or None)
        q_consumer = (consumer != '*' and consumer or None)

        curs = self.db.cursor()
        q = "select * from pgq.get_consumer_info(%s, %s)"
        curs.execute(q, [q_queue, q_consumer])

        display_result(curs, 'Consumer "%s" on queue "%s":' % (consumer, queue))

    def cmd_show_node(self, params):
        """Show node information."""

        # TODO: This should additionally show node roles, lags and hierarchy.
        # Similar to londiste "status".

        node = params.get('node', '*')
        queue = params.get('queue', '*')

        q_queue = (queue != '*' and queue or None)
        q_node = (node != '*' and node or None)

        curs = self.db.cursor()
        q = """select queue_name, node_name, node_location, dead
               from pgq_node.node_location
               where node_name = coalesce(%s, node_name)
                     and queue_name = coalesce(%s, queue_name)
               order by 1,2"""
        curs.execute(q, [q_node, q_queue])

        display_result(curs, 'Node "%s" on queue "%s":' % (node, queue))

    def cmd_show_batch(self, params):
        batch_id = params.get('batch_id')
        consumer = params.get('consumer')
        queue = self.cur_queue
        if not queue:
            print 'No default queue'
            return
        curs = self.db.cursor()
        if consumer:
            q = "select current_batch from pgq.get_consumer_info(%s, %s)"
            curs.execute(q, [queue, consumer])
            res = curs.fetchall()
            if len(res) != 1:
                print 'no such consumer'
                return
            batch_id = res[0]['current_batch']
            if batch_id is None:
                print 'consumer has no open batch'
                return

        q = "select * from pgq.get_batch_events(%s)"
        curs.execute(q, [batch_id])

        display_result(curs, 'Batch events:')

    def cmd_register_consumer(self, params):
        queue = params.get("queue", self.cur_queue)
        if not queue:
            print 'No queue specified'
            return
        at_tick = params.get('at_tick')
        copy_reg = params.get('copy_reg')
        consumer = params['consumer']
        curs = self.db.cursor()

        # copy other registration
        if copy_reg:
            q = "select coalesce(next_tick, last_tick) as pos from pgq.get_consumer_info(%s, %s)"
            curs.execute(q, [queue, copy_reg])
            reg = curs.fetchone()
            if not reg:
                print "Consumer %s not registered on queue %d" % (copy_reg, queue)
                return
            at_tick = reg['pos']

        # avoid double reg if specific pos is not requested
        if not at_tick:
            q = "select * from pgq.get_consumer_info(%s, %s)"
            curs.execute(q, [queue, consumer])
            if curs.fetchone():
                print 'Consumer already registered'
                return

        if at_tick:
            q = "select * from pgq.register_consumer_at(%s, %s, %s)"
            curs.execute(q, [queue, consumer, int(at_tick)])
        else:
            q = "select * from pgq.register_consumer(%s, %s)"
            curs.execute(q, [queue, consumer])
        print "REGISTER"

    def cmd_register_subconsumer(self, params):
        queue = params.get("queue", self.cur_queue)
        if not queue:
            print 'No queue specified'
            return
        subconsumer = params['subconsumer']
        consumer = params.get("consumer")
        if not consumer:
            print 'No consumer specified'
            return
        curs = self.db.cursor()

        _subcon_name = '%s.%s' % (consumer, subconsumer)

        q = "select * from pgq.get_consumer_info(%s, %s)"
        curs.execute(q, [queue, _subcon_name])
        if curs.fetchone():
            print 'Subconsumer already registered'
            return

        q = "select * from pgq_coop.register_subconsumer(%s, %s, %s)"
        curs.execute(q, [queue, consumer, subconsumer])
        print "REGISTER"

    def cmd_unregister_consumer(self, params):
        queue = params.get("queue", self.cur_queue)
        if not queue:
            print 'No queue specified'
            return
        consumer = params['consumer']
        curs = self.db.cursor()
        if consumer == '*':
            q = 'select consumer_name from pgq.get_consumer_info(%s)'
            curs.execute(q, [queue])
            consumers = [row['consumer_name'] for row in curs.fetchall()]
        else:
            consumers = [consumer]
        q = "select * from pgq.unregister_consumer(%s, %s)"
        for consumer in consumers:
            curs.execute(q, [queue, consumer])
        print "UNREGISTER"

    def cmd_unregister_subconsumer(self, params):
        queue = params.get("queue", self.cur_queue)
        if not queue:
            print 'No queue specified'
            return
        subconsumer = params["subconsumer"]
        consumer = params['consumer']
        batch_handling = int(params.get('close') is not None)
        curs = self.db.cursor()
        if subconsumer == '*':
            q = 'select consumer_name from pgq.get_consumer_info(%s)'
            curs.execute(q, [queue])
            subconsumers = [row['consumer_name'].split('.')[1]
                           for row in curs.fetchall()
                           if row['consumer_name'].startswith('%s.' % consumer)]
        else:
            subconsumers = [subconsumer]
        q = "select * from pgq_coop.unregister_subconsumer(%s, %s, %s, %s)"
        for subconsumer in subconsumers:
            curs.execute(q, [queue, consumer, subconsumer, batch_handling])
        print "UNREGISTER"

    def cmd_create_queue(self, params):
        curs = self.db.cursor()
        q = "select * from pgq.get_queue_info(%(queue)s)"
        curs.execute(q, params)
        if curs.fetchone():
            print "Queue already exists"
            return
        q = "select * from pgq.create_queue(%(queue)s)"
        curs.execute(q, params)
        print "CREATE"

    def cmd_drop_queue(self, params):
        curs = self.db.cursor()
        q = "select * from pgq.drop_queue(%(queue)s)"
        curs.execute(q, params)
        print "DROP"

    def cmd_alter_queue(self, params):
        """Alter queue parameters, accepts * for all queues"""
        queue = params.get('queue')
        curs = self.db.cursor()
        if queue == '*':
            # operate on list of queues
            q = "select queue_name from pgq.get_queue_info()"
            curs.execute(q)
            qlist = [ r[0] for r in curs.fetchall() ]
        else:
            # just single queue specified
            qlist = [ queue ]

        for qname in qlist:
            params['queue'] = qname

            # loop through the parameters, passing any unrecognized
            # key down pgq.set_queue_config
            for k in params:
                if k in ('queue', 'cmd', 'cmd2'):
                    continue

                q = "select * from pgq.set_queue_config" \
                    "(%%(queue)s, '%s', %%(%s)s)" % (k, k)

                curs.execute(q, params)
        print "ALTER"

    def cmd_show_help(self, params):
        print __doc__

    def cmd_exit(self, params):
        sys.exit(0)

    ##
    ## Londiste
    ##

    def cmd_londiste_missing(self, params):
        """Show missing objects."""

        queue = self.cur_queue

        curs = self.db.cursor()
        q = """select * from londiste.local_show_missing(%s)"""
        curs.execute(q, [queue])

        display_result(curs, 'Missing objects on queue "%s":' % (queue))

    def cmd_londiste_tables(self, params):
        """Show local tables."""

        queue = self.cur_queue

        curs = self.db.cursor()
        q = """select * from londiste.get_table_list(%s) where local"""
        curs.execute(q, [queue])

        display_result(curs, 'Local tables on queue "%s":' % (queue))

    def cmd_londiste_seqs(self, params):
        """Show local seqs."""

        queue = self.cur_queue

        curs = self.db.cursor()
        q = """select * from londiste.get_seq_list(%s) where local"""
        curs.execute(q, [queue])

        display_result(curs, 'Sequences on queue "%s":' % (queue))

    def cmd_londiste_add_table(self, params):
        """Add table."""

        args = []
        for a in ('skip_truncate', 'expect_sync', 'backup', 'no_triggers', 'skip'):
            if a in params:
                args.append(a)
        for a in ('tgflags', 'ignore', 'pkey', 'when',
                  'ev_type', 'ev_data',
                  'ev_extra1', 'ev_extra2', 'ev_extra3', 'ev_extra4'):
            if a in params:
                args.append("%s=%s" % (a, params[a]))

        curs = self.db.cursor()
        q = """select * from londiste.local_add_table(%s, %s, %s)"""
        for tbl in params['tables']:
            curs.execute(q, [self.cur_queue, tbl, args])
            res = curs.fetchone()
            print res[0], res[1]
        print 'ADD_TABLE'

    def cmd_londiste_remove_table(self, params):
        """Remove table."""

        curs = self.db.cursor()
        q = """select * from londiste.local_remove_table(%s, %s)"""
        for tbl in params['tables']:
            curs.execute(q, [self.cur_queue, tbl])
            res = curs.fetchone()
            print res[0], res[1]
        print 'REMOVE_TABLE'

    def cmd_londiste_add_seq(self, params):
        """Add seq."""

        curs = self.db.cursor()
        q = """select * from londiste.local_add_seq(%s, %s)"""
        for seq in params['seqs']:
            curs.execute(q, [self.cur_queue, seq])
            res = curs.fetchone()
            print res[0], res[1]
        print 'ADD_SEQ'

    def cmd_londiste_remove_seq(self, params):
        """Remove seq."""

        curs = self.db.cursor()
        q = """select * from londiste.local_remove_seq(%s, %s)"""
        for seq in params['seqs']:
            curs.execute(q, [self.cur_queue, seq])
            res = curs.fetchone()
            print res[0], res[1]
        print 'REMOVE_SEQ:', res[0], res[1]

    ## generic info

    def cmd_show_table(self, params):
        print '-' * 64
        tbl = params['table']
        curs = self.db.cursor()
        s = skytools.TableStruct(curs, tbl)
        s.create(fakecurs(), skytools.T_ALL)
        print '-' * 64

    def cmd_show_sequence(self, params):
        print '-' * 64
        seq = params['seq']
        curs = self.db.cursor()
        s = skytools.SeqStruct(curs, seq)
        s.create(fakecurs(), skytools.T_ALL)
        print '-' * 64

    ## sql pass-through

    def execute_sql(self, params):
        tks = [tk[1] for tk in self.tokens]
        sql = ' '.join(tks)

        curs = self.db.cursor()
        curs.execute(sql)

        if curs.description:
            display_result(curs, None)
        print curs.statusmessage

class fakecurs:
    def execute(self, sql):
        print sql

def main():
    global script
    script = AdminConsole()
    script.run(sys.argv[1:])

if __name__ == '__main__':
    main()
