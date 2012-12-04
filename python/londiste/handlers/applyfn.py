"""
Send all events to a DB function.
"""

import skytools
from londiste.handler import BaseHandler

__all__ = ['ApplyFuncHandler']

class ApplyFuncHandler(BaseHandler):
    """Call DB function to apply event.

    Parameters:
      func_name=NAME - database function name
      func_conf=CONF - database function conf
    """
    handler_name = 'applyfn'

    def prepare_batch(self, batch_info, dst_curs):
        self.cur_tick = batch_info['tick_id']

    def process_event(self, ev, sql_queue_func, qfunc_arg):
        """Ignore events for this table"""
        fn = self.args.get('func_name')
        fnconf = self.args.get('func_conf', '')

        args = [fnconf, self.cur_tick,
                ev.ev_id, ev.ev_time,
                ev.ev_txid, ev.ev_retry,
                ev.ev_type, ev.ev_data,
                ev.ev_extra1, ev.ev_extra2,
                ev.ev_extra3, ev.ev_extra4]

        qfn = skytools.quote_fqident(fn)
        qargs = [skytools.quote_literal(a) for a in args]
        sql = "select %s(%s);" % (qfn, ', '.join(qargs))
        self.log.debug('applyfn.sql: %s' % sql)
        sql_queue_func(sql, qfunc_arg)

#------------------------------------------------------------------------------
# register handler class
#------------------------------------------------------------------------------

__londiste_handlers__ = [ApplyFuncHandler]
