
"""PgQ event container.
"""

__all__ = ['EV_UNTAGGED', 'EV_RETRY', 'EV_DONE', 'Event']

# Event status codes
EV_UNTAGGED = -1
EV_RETRY = 0
EV_DONE = 1

_fldmap = {
        'ev_id': 'ev_id',
        'ev_txid': 'ev_txid',
        'ev_time': 'ev_time',
        'ev_type': 'ev_type',
        'ev_data': 'ev_data',
        'ev_extra1': 'ev_extra1',
        'ev_extra2': 'ev_extra2',
        'ev_extra3': 'ev_extra3',
        'ev_extra4': 'ev_extra4',

        'id': 'ev_id',
        'txid': 'ev_txid',
        'time': 'ev_time',
        'type': 'ev_type',
        'data': 'ev_data',
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
    __slots__ = ('_event_row', '_status', 'retry_time',
                 'queue_name')

    def __init__(self, queue_name, row):
        self._event_row = row
        self._status = EV_UNTAGGED
        self.retry_time = 60
        self.queue_name = queue_name

    def __getattr__(self, key):
        return self._event_row[_fldmap[key]]

    def tag_done(self):
        self._status = EV_DONE

    def tag_retry(self, retry_time = 60):
        self._status = EV_RETRY
        self.retry_time = retry_time

    def get_status(self):
        return self._status

    # be also dict-like
    def __getitem__(self, k): return self._event_row.__getitem__(k)
    def __contains__(self, k): return self._event_row.__contains__(k)
    def get(self, k, d=None): return self._event_row.get(k, d)
    def has_key(self, k): return self._event_row.has_key(k)
    def keys(self): return self._event_row.keys()
    def values(self): return self._event_row.keys()
    def items(self): return self._event_row.items()
    def iterkeys(self): return self._event_row.iterkeys()
    def itervalues(self): return self._event_row.itervalues()
    def __str__(self):
        return "<id=%d type=%s data=%s e1=%s e2=%s e3=%s e4=%s>" % (
                self.id, self.type, self.data, self.extra1, self.extra2, self.extra3, self.extra4)
