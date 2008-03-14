
"""PgQ producer helpers for Python.
"""

import skytools

__all__ = ['bulk_insert_events', 'insert_event']

_fldmap = {
    'id': 'ev_id',
    'time': 'ev_time',
    'type': 'ev_type',
    'data': 'ev_data',
    'extra1': 'ev_extra1',
    'extra2': 'ev_extra2',
    'extra3': 'ev_extra3',
    'extra4': 'ev_extra4',

    'ev_id': 'ev_id',
    'ev_time': 'ev_time',
    'ev_type': 'ev_type',
    'ev_data': 'ev_data',
    'ev_extra1': 'ev_extra1',
    'ev_extra2': 'ev_extra2',
    'ev_extra3': 'ev_extra3',
    'ev_extra4': 'ev_extra4',
}

def bulk_insert_events(curs, rows, fields, queue_name):
    q = "select pgq.current_event_table(%s)"
    curs.execute(q, [queue_name])
    tbl = curs.fetchone()[0]
    db_fields = map(_fldmap.get, fields)
    skytools.magic_insert(curs, tbl, rows, db_fields)

def insert_event(curs, queue, ev_type, ev_data,
                 extra1=None, extra2=None,
                 extra3=None, extra4=None):
    q = "select pgq.insert_event(%s, %s, %s, %s, %s, %s, %s)"
    curs.execute(q, [queue, ev_type, ev_data,
                     extra1, extra2, extra3, extra4])
    return curs.fetchone()[0]

