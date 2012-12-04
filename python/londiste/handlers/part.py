"""Event filtering by hash, for partitioned databases.

Parameters:
  key=COLUMN: column name to use for hashing
  hashfunc=NAME: function to use for hashing (default: partconf.get_hash_raw)
  hashexpr=EXPR: full expression to use for hashing (deprecated)
  encoding=ENC: validate and fix incoming data (only utf8 supported atm)

On root node:
* Hash of key field will be added to ev_extra3.
  This is implemented by adding additional trigger argument:
        ev_extra3='hash='||partconf.get_hash_raw(key_column)

On branch/leaf node:
* On COPY time, the SELECT on provider side gets filtered by hash.
* On replay time, the events gets filtered by looking at hash in ev_extra3.

Local config:
* Local hash value and mask are loaded from partconf.conf table.

"""

import skytools
from londiste.handler import TableHandler

__all__ = ['PartHandler']

class PartHandler(TableHandler):
    __doc__ = __doc__
    handler_name = 'part'

    DEFAULT_HASHFUNC = "partconf.get_hash_raw"
    DEFAULT_HASHEXPR = "%s(%s)"

    def __init__(self, table_name, args, dest_table):
        TableHandler.__init__(self, table_name, args, dest_table)
        self.max_part = None       # max part number
        self.local_part = None     # part number of local node

        # primary key columns
        self.key = args.get('key')
        if self.key is None:
            raise Exception('Specify key field as key argument')

        # hash function & full expression
        hashfunc = args.get('hashfunc', self.DEFAULT_HASHFUNC)
        self.hashexpr = self.DEFAULT_HASHEXPR % (
                skytools.quote_fqident(hashfunc),
                skytools.quote_ident(self.key))
        self.hashexpr = args.get('hashexpr', self.hashexpr)

    def reset(self):
        """Forget config info."""
        self.max_part = None
        self.local_part = None
        TableHandler.reset(self)

    def add(self, trigger_arg_list):
        """Let trigger put hash into extra3"""

        arg = "ev_extra3='hash='||%s" % self.hashexpr
        trigger_arg_list.append(arg)
        TableHandler.add(self, trigger_arg_list)

    def prepare_batch(self, batch_info, dst_curs):
        """Called on first event for this table in current batch."""
        if not self.max_part:
            self.load_part_info(dst_curs)
        TableHandler.prepare_batch(self, batch_info, dst_curs)

    def process_event(self, ev, sql_queue_func, arg):
        """Filter event by hash in extra3, apply only local part."""
        if ev.extra3:
            meta = skytools.db_urldecode(ev.extra3)
            self.log.debug('part.process_event: hash=%d, max_part=%s, local_part=%d' %\
                           (int(meta['hash']), self.max_part, self.local_part))
            if (int(meta['hash']) & self.max_part) != self.local_part:
                self.log.debug('part.process_event: not my event')
                return
        self.log.debug('part.process_event: my event, processing')
        TableHandler.process_event(self, ev, sql_queue_func, arg)

    def get_copy_condition(self, src_curs, dst_curs):
        """Prepare the where condition for copy and replay filtering"""
        self.load_part_info(dst_curs)
        w = "(%s & %d) = %d" % (self.hashexpr, self.max_part, self.local_part)
        self.log.debug('part: copy_condition=%s' % w)
        return w

    def load_part_info(self, curs):
        """Load slot info from database."""
        q = "select part_nr, max_part from partconf.conf"
        curs.execute(q)
        self.local_part, self.max_part = curs.fetchone()
        if self.local_part is None or self.max_part is None:
            raise Exception('Error loading part info')

# register handler class
__londiste_handlers__ = [PartHandler]
