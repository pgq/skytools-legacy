"""
Dummy handler to setup queue tables. All events are ignored.
"""

from londiste.handler import BaseHandler

__all__ = ['QueueTableHandler']

class QueueTableHandler(BaseHandler):
    """Queue table handler. Do nothing"""
    handler_name = 'qtable'

    def add(self, trigger_arg_list):
        """Create SKIP and BEFORE INSERT trigger"""
        trigger_arg_list.append('tgflags=BI')
        trigger_arg_list.append('SKIP')

    def process_event(self, ev, sql_queue_func, arg):
        """Ignore events for this table"""

__londiste_handlers__ = [QueueTableHandler]

