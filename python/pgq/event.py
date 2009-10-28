
"""PgQ event container.
"""

__all__ = ['EV_RETRY', 'EV_DONE', 'EV_FAILED', 'Event']

# Event status codes
EV_RETRY = 0
EV_DONE = 1
EV_FAILED = 2

_fldmap = {
        'ev_id': 'ev_id',
        'ev_txid': 'ev_txid',
        'ev_time': 'ev_time',
        'ev_type': 'ev_type',
        'ev_data': 'ev_data',
        'ev_retry': 'ev_retry',
        'ev_extra1': 'ev_extra1',
        'ev_extra2': 'ev_extra2',
        'ev_extra3': 'ev_extra3',
        'ev_extra4': 'ev_extra4',

        'id': 'ev_id',
        'txid': 'ev_txid',
        'time': 'ev_time',
        'type': 'ev_type',
        'data': 'ev_data',
        'retry': 'ev_retry',
        'extra1': 'ev_extra1',
        'extra2': 'ev_extra2',
        'extra3': 'ev_extra3',
        'extra4': 'ev_extra4',
}

class Event(object):
    """Event data for consumers.
    
    Consumer is supposed to tag them after processing.
    If not, events will stay in retry queue.
    """
    def __init__(self, queue_name, row):
        self._event_row = row
        self.status = EV_RETRY
        self.retry_time = 60
        self.fail_reason = "Buggy consumer"
        self.queue_name = queue_name

    def __getattr__(self, key):
        return self._event_row[_fldmap[key]]

    def tag_done(self):
        self.status = EV_DONE

    def tag_retry(self, retry_time = 60):
        self.status = EV_RETRY
        self.retry_time = retry_time

    def tag_failed(self, reason):
        self.status = EV_FAILED
        self.fail_reason = reason

