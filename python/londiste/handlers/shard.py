"""Event filtering by hash, for partitioned databases.

Parameters:
  key=COLUMN: column name to use for hashing
  hash_key=COLUMN: column name to use for hashing (overrides 'key' parameter)
  hashfunc=NAME: function to use for hashing (default: partconf.get_hash_raw)
  hashexpr=EXPR: full expression to use for hashing (deprecated)
  encoding=ENC: validate and fix incoming data (only utf8 supported atm)
  ignore_truncate=BOOL: ignore truncate event, default: 0, values: 0,1

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

__all__ = ['ShardHandler', 'PartHandler']

class ShardHandler (TableHandler):
    __doc__ = __doc__
    handler_name = 'shard'

    DEFAULT_HASHFUNC = "partconf.get_hash_raw"
    DEFAULT_HASHEXPR = "%s(%s)"

    def __init__(self, table_name, args, dest_table):
        TableHandler.__init__(self, table_name, args, dest_table)
        self.hash_mask = None   # aka max part number (atm)
        self.shard_nr = None    # part number of local node

        # primary key columns
        self.hash_key = args.get('hash_key', args.get('key'))
        self._validate_hash_key()

        # hash function & full expression
        hashfunc = args.get('hashfunc', self.DEFAULT_HASHFUNC)
        self.hashexpr = self.DEFAULT_HASHEXPR % (
                skytools.quote_fqident(hashfunc),
                skytools.quote_ident(self.hash_key or ''))
        self.hashexpr = args.get('hashexpr', self.hashexpr)

    def _validate_hash_key(self):
        if self.hash_key is None:
            raise Exception('Specify hash key field as hash_key argument')

    def reset(self):
        """Forget config info."""
        self.hash_mask = None
        self.shard_nr = None
        TableHandler.reset(self)

    def add(self, trigger_arg_list):
        """Let trigger put hash into extra3"""
        arg = "ev_extra3='hash='||%s" % self.hashexpr
        trigger_arg_list.append(arg)
        TableHandler.add(self, trigger_arg_list)

    def prepare_batch(self, batch_info, dst_curs):
        """Called on first event for this table in current batch."""
        if self.hash_key is not None:
            if not self.hash_mask:
                self.load_shard_info(dst_curs)
        TableHandler.prepare_batch(self, batch_info, dst_curs)

    def process_event(self, ev, sql_queue_func, arg):
        """Filter event by hash in extra3, apply only if for local shard."""
        if ev.extra3 and self.hash_key is not None:
            meta = skytools.db_urldecode(ev.extra3)
            self.log.debug('shard.process_event: hash=%i, hash_mask=%i, shard_nr=%i',
                           int(meta['hash']), self.hash_mask, self.shard_nr)
            if (int(meta['hash']) & self.hash_mask) != self.shard_nr:
                self.log.debug('shard.process_event: not my event')
                return
        self._process_event(ev, sql_queue_func, arg)

    def _process_event(self, ev, sql_queue_func, arg):
        self.log.debug('shard.process_event: my event, processing')
        TableHandler.process_event(self, ev, sql_queue_func, arg)

    def get_copy_condition(self, src_curs, dst_curs):
        """Prepare the where condition for copy and replay filtering"""
        if self.hash_key is None:
            return TableHandler.get_copy_condition(self, src_curs, dst_curs)
        self.load_shard_info(dst_curs)
        w = "(%s & %d) = %d" % (self.hashexpr, self.hash_mask, self.shard_nr)
        self.log.debug('shard: copy_condition=%r', w)
        return w

    def load_shard_info(self, curs):
        """Load part/slot info from database."""
        q = "select part_nr, max_part from partconf.conf"
        curs.execute(q)
        self.shard_nr, self.hash_mask = curs.fetchone()
        if self.shard_nr is None or self.hash_mask is None:
            raise Exception('Error loading shard info')

class PartHandler (ShardHandler):
    __doc__ = "Deprecated compat name for shard handler.\n" + __doc__.split('\n',1)[1]
    handler_name = 'part'

# register handler class
__londiste_handlers__ = [ShardHandler, PartHandler]
