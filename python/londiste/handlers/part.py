"""
Experimental event filtering by hash.
"""

import skytools
from londiste.handler import TableHandler

__all__ = ['PartHandler']

class PartHandler(TableHandler):
    handler_name = 'part'

    def __init__(self, table_name, args, log):
        TableHandler.__init__(self, table_name, args, log)
        self.max_part = None       # max part number
        self.local_part = None     # part number of local node
        self.key = args.get('key')        
        if self.key is None:
            raise Exception('Specify key field as key agument')

    def reset(self):
        """Forget config info."""
        self.max_part = None
        self.local_part = None
        TableHandler.reset(self)

    def add(self, trigger_arg_list):
        """Let trigger put hash into extra3"""

        arg = "ev_extra3='hash='||hashtext(%s)" % skytools.quote_ident(self.key)
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

    def real_copy(self, tablename, src_curs, dst_curs, column_list, cond_list):
        """Copy only slots needed locally."""
        self.load_part_info(dst_curs)
        fn = 'hashtext(%s)' % skytools.quote_ident(self.key)
        w = "%s & %d = %d" % (fn, self.max_part, self.local_part)
        self.log.debug('part: copy_condition=%s' % w)
        cond_list.append(w)

        return TableHandler.real_copy(self, tablename, src_curs, dst_curs,
                                     column_list, cond_list)

    def load_part_info(self, curs):
        """Load slot info from database."""
        q = "select part_nr, max_part from partconf.conf"
        curs.execute(q)
        self.local_part, self.max_part = curs.fetchone()
        if self.local_part is None or self.max_part is None:
            raise Exeption('Error loading part info')

# register handler class
__londiste_handlers__ = [PartHandler]

