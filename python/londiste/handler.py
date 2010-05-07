
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

import sys, skytools

__all__ = ['BaseHandler', 'parse_handler', 'build_handler', 'load_handlers']

class BaseHandler:
    handler_name = 'fwd'
    def __init__(self, name, next, args):
        self.name = name
        self.next = next
        self.args = args

    def add(self, trigger_arg_list):
        """Called when table is added.

        Can modify trigger args.
        """
        if self.next:
            self.next.add(trigger_arg_list)

    def reset(self):
        """Called before starting to process a batch.
        Should clean any pending data.
        """
        if self.next:
            self.next.reset()

    def prepare_batch(self, batch_info, dst_curs):
        """Called on first event for this table in current batch."""
        if self.next:
            self.next.prepare_batch(batch_info, dst_curs)

    def process_event(self, ev, sql_queue_func, arg):
        """Process a event.
        
        Event should be added to sql_queue or executed directly.
        """
        if self.next:
            self.next.process_event(ev, sql_queue_func, arg)

    def finish_batch(self, batch_info):
        """Called when batch finishes."""
        if self.next:
            self.next.finish_batch(batch_info)

    def prepare_copy(self, expr_list, dst_curs):
        """Can change COPY behaviour.
        
        Returns new expr.
        """
        if self.next:
            self.next.prepare_copy(expr_list, dst_curs)

class TableHandler(BaseHandler):
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

def build_handler(tblname, hlist):
    """Execute array of handler initializers."""
    klist = []
    for h in hlist:
        if not h:
            continue
        pos = h.find('(')
        if pos >= 0:
            if h[-1] != ')':
                raise Exception("handler fmt error")
            name = h[:pos].strip()
            args = h[pos+1 : -1].split(',')
            args = [a.strip() for a in args]
        else:
            name = h
            args = []

        klass = _handler_map[name]
        klist.append( (klass, args) )

    # always append default handler
    klist.append( (TableHandler, []) )

    # link them together
    p = None
    klist.reverse()
    for klass, args in klist:
        p = klass(tblname, p, args)
    return p

def parse_handler(tblname, hstr):
    """Parse and execute string of colon-separated handler initializers."""
    hlist = hstr.split(':')
    return build_handler(tblname, hlist)

def load_handlers(cf):
    """Load and register modules from config."""
    lst = cf.getlist('handler_modules', [])

    for m in lst:
        register_handler_module(m)

