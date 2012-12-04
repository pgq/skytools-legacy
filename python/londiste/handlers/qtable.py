"""

Handlers:

qtable     - dummy handler to setup queue tables. All events are ignored. Use in
             root node.
fake_local - dummy handler to setup queue tables. All events are ignored. Table
             structure is not required. Use in branch/leaf.
qsplitter  - dummy handler to setup queue tables. All events are ignored. Table
             structure is not required. All table events are inserted to
             destination queue, specified with handler arg 'queue'.

"""

from londiste.handler import BaseHandler

import pgq

__all__ = ['QueueTableHandler', 'QueueSplitterHandler']


class QueueTableHandler(BaseHandler):
    """Queue table handler. Do nothing.

    Trigger: before-insert, skip trigger.
    Event-processing: do nothing.
    """
    handler_name = 'qtable'

    def add(self, trigger_arg_list):
        """Create SKIP and BEFORE INSERT trigger"""
        trigger_arg_list.append('tgflags=BI')
        trigger_arg_list.append('SKIP')
        trigger_arg_list.append('expect_sync')

    def real_copy(self, tablename, src_curs, dst_curs, column_list):
        """Force copy not to start"""
        return (0,0)

    def needs_table(self):
        return False

class QueueSplitterHandler(BaseHandler):
    """Send events for one table to another queue.

    Parameters:
      queue=QUEUE - Queue name.
    """
    handler_name = 'qsplitter'

    def __init__(self, table_name, args, dest_table):
        """Init per-batch table data cache."""
        BaseHandler.__init__(self, table_name, args, dest_table)
        try:
            self.dst_queue_name = args['queue']
        except KeyError:
            raise Exception('specify queue with handler-arg')
        self.rows = []

    def add(self, trigger_arg_list):
        trigger_arg_list.append('virtual_table')

    def prepare_batch(self, batch_info, dst_curs):
        """Called on first event for this table in current batch."""
        self.rows = []

    def process_event(self, ev, sql_queue_func, arg):
        """Process a event.

        Event should be added to sql_queue or executed directly.
        """
        if self.dst_queue_name is None: return

        data = [ev.type, ev.data,
                ev.extra1, ev.extra2, ev.extra3, ev.extra4, ev.time]
        self.rows.append(data)

    def finish_batch(self, batch_info, dst_curs):
        """Called when batch finishes."""
        if self.dst_queue_name is None: return

        fields = ['type', 'data',
                  'extra1', 'extra2', 'extra3', 'extra4', 'time']
        pgq.bulk_insert_events(dst_curs, self.rows, fields, self.dst_queue_name)

    def needs_table(self):
        return False


__londiste_handlers__ = [QueueTableHandler, QueueSplitterHandler]
