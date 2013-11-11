
"""PgQ event container.
"""

__all__ = ['Event']

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
        'ev_retry': 'ev_retry',

        'id': 'ev_id',
        'txid': 'ev_txid',
        'time': 'ev_time',
        'type': 'ev_type',
        'data': 'ev_data',
        'extra1': 'ev_extra1',
        'extra2': 'ev_extra2',
        'extra3': 'ev_extra3',
        'extra4': 'ev_extra4',
        'retry': 'ev_retry',
}

class Event(object):
    """Event data for consumers.

    Will be removed from the queue by default.
    """
    __slots__ = ('_event_row', 'retry_time', 'queue_name')

    def __init__(self, queue_name, row):
        self._event_row = row
        self.retry_time = 60
        self.queue_name = queue_name

    def __getattr__(self, key):
        return self._event_row[_fldmap[key]]

    # would be better in RetriableEvent only since we don't care but
    # unfortunately it needs to be defined here due to compatibility concerns
    def tag_done(self):
        pass

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
