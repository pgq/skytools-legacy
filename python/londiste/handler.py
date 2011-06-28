
"""Table handler.

Per-table decision how to create trigger, copy data and apply events.
"""

"""
-- redirect & create table
partition by batch_time
partition by date field

-- sql handling:
cube1 - I/U/D -> partition, insert
cube2 - I/U/D -> partition, del/insert
field remap
name remap

bublin filter
- replay: filter events
- copy: additional where
- add: add trigger args

multimaster
- replay: conflict handling, add fncall to sql queue?
- add: add 'backup' arg to trigger

plain londiste:
- replay: add to sql queue

"""

import sys, skytools, londiste.handlers

__all__ = ['RowCache', 'BaseHandler', 'build_handler',
           'load_handler_modules', 'create_handler_string']

class RowCache:
    def __init__(self, table_name):
        self.table_name = table_name
        self.keys = {}
        self.rows = []

    def add_row(self, d):
        row = [None] * len(self.keys)
        for k, v in d.items():
            try:
                row[self.keys[k]] = v
            except KeyError:
                i = len(row)
                self.keys[k] = i
                row.append(v)
        row = tuple(row)
        self.rows.append(row)

    def get_fields(self):
        row = [None] * len(self.keys)
        for k, i in self.keys.keys():
            row[i] = k
        return tuple(row)

    def apply_rows(self, curs):
        fields = self.get_fields()
        skytools.magic_insert(curs, self.table_name, self.rows, fields)

class BaseHandler:
    """Defines base API, does nothing.
    """
    handler_name = 'nop'
    def __init__(self, table_name, args, log):
        self.table_name = table_name
        self.args = args
        self.log = log

    def add(self, trigger_arg_list):
        """Called when table is added.

        Can modify trigger args.
        """
        pass

    def reset(self):
        """Called before starting to process a batch.
        Should clean any pending data.
        """
        pass

    def prepare_batch(self, batch_info, dst_curs):
        """Called on first event for this table in current batch."""
        pass

    def process_event(self, ev, sql_queue_func, arg):
        """Process a event.

        Event should be added to sql_queue or executed directly.
        """
        pass

    def finish_batch(self, batch_info, dst_curs):
        """Called when batch finishes."""
        pass

    def real_copy(self, tablename, src_curs, dst_curs, column_list, cond_list):
        """do actual table copy and return tuple with number of bytes and rows
        copyed
        """
        condition = ' and '.join(cond_list)
        return skytools.full_copy(tablename, src_curs, dst_curs, column_list,
                                  condition)

    def needs_table(self):
        """Does the handler need the table to exist on destination."""
        return True

class TableHandler(BaseHandler):
    """Default Londiste handler, inserts events into tables with plain SQL."""
    handler_name = 'londiste'

    sql_command = {
        'I': "insert into %s %s;",
        'U': "update only %s set %s;",
        'D': "delete from only %s where %s;",
    }

    def process_event(self, ev, sql_queue_func, arg):
        if len(ev.type) == 1:
            # sql event
            fqname = skytools.quote_fqident(ev.extra1)
            fmt = self.sql_command[ev.type]
            sql = fmt % (fqname, ev.data)
        else:
            # urlenc event
            pklist = ev.type[2:].split(',')
            row = skytools.db_urldecode(ev.data)
            op = ev.type[0]
            tbl = ev.extra1
            if op == 'I':
                sql = skytools.mk_insert_sql(row, tbl, pklist)
            elif op == 'U':
                sql = skytools.mk_update_sql(row, tbl, pklist)
            elif op == 'D':
                sql = skytools.mk_delete_sql(row, tbl, pklist)

        sql_queue_func(sql, arg)

_handler_map = {
    'londiste': TableHandler,
}

def register_handler_module(modname):
    """Import and module and register handlers."""
    __import__(modname)
    m = sys.modules[modname]
    for h in m.__londiste_handlers__:
        _handler_map[h.handler_name] = h

def _parse_arglist(arglist):
    args = {}
    for arg in arglist or []:
        key, _, val = arg.partition('=')
        if key in args:
            raise Exception('multiple handler arguments: %s' % key)
        args[key] = val.strip()
    return args

def create_handler_string(name, arglist):
    handler = name
    if arglist:
        args = _parse_arglist(arglist)
        astr = skytools.db_urlencode(args)
        handler = '%s(%s)' % (handler, astr)
    return handler

def _parse_handler(hstr):
    """Parse result of create_handler_string()."""
    args = {}
    name = hstr
    pos = hstr.find('(')
    if pos > 0:
        name = hstr[ : pos]
        if hstr[-1] != ')':
            raise Exception('invalid handler format: %s' % hstr)
        astr = hstr[pos + 1 : -1]
        if astr:
            astr = astr.replace(',', '&')
            args = skytools.db_urldecode(astr)
    return (name, args)

def build_handler(tblname, hstr, log):
    """Parse and initialize handler.

    hstr is result of create_handler_string()."""
    hname, args = _parse_handler(hstr)
    # when no handler specified, use londiste
    hname = hname or 'londiste'
    klass = _handler_map[hname]
    return klass(tblname, args, log)

def load_handler_modules(cf):
    """Load and register modules from config."""
    lst = londiste.handlers.DEFAULT_HANDLERS
    lst += cf.getlist('handler_modules', [])

    for m in lst:
        register_handler_module(m)

