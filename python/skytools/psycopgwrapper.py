
"""Wrapper around psycopg1/2.

Preferred is psycopg2, fallback to psycopg1.

Interface provided is psycopg1:
    - dict* methods.
    - new columns can be assigned to row.

"""

import sys

__all__ = []

##from psycopg2.psycopg1 import connect as _pgconnect
# psycopg2.psycopg1.cursor is too backwards compatible,
# to the point of avoiding optimized access.
# only backwards compat thing we need is dict* methods

import psycopg2.extensions, psycopg2.extras
from skytools.sqltools import dbdict

class _CompatRow(psycopg2.extras.DictRow):
    """Make DictRow more dict-like."""
    __slots__ = ('_index',)

    def __contains__(self, k):
        """Returns if such row has such column."""
        return k in self._index

    def copy(self):
        """Return regular dict."""
        return dbdict(self.iteritems())
    
    def iterkeys(self):
        return self._index.iterkeys()

    def itervalues(self):
        return list.__iter__(self)

    # obj.foo access
    def __getattr__(self, k):
        return self[k]

class _CompatCursor(psycopg2.extras.DictCursor):
    """Regular psycopg2 DictCursor with dict* methods."""
    def __init__(self, *args, **kwargs):
        psycopg2.extras.DictCursor.__init__(self, *args, **kwargs)
        self.row_factory = _CompatRow
    dictfetchone = psycopg2.extras.DictCursor.fetchone
    dictfetchall = psycopg2.extras.DictCursor.fetchall
    dictfetchmany = psycopg2.extras.DictCursor.fetchmany

class _CompatConnection(psycopg2.extensions.connection):
    """Connection object that uses _CompatCursor."""
    def cursor(self):
        return psycopg2.extensions.connection.cursor(self, cursor_factory = _CompatCursor)

def connect_database(connstr):
    """Create a db connection with connect_timeout option.
    
    Default connect_timeout is 15, to change put it directly into dsn.
    """

    # allow override
    if connstr.find("connect_timeout") < 0:
        connstr += " connect_timeout=15"

    # create connection
    return _CompatConnection(connstr)

