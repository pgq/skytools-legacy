
"""Wrapper around psycopg1/2.

Preferred is psycopg2, fallback to psycopg1.

Interface provided is psycopg1:
    - dict* methods.
    - new columns can be assigned to row.

"""

import sys

__all__ = ['I_AUTOCOMMIT', 'I_READ_COMMITTED', 'I_REPEATABLE_READ', 'I_SERIALIZABLE']

try:
    ##from psycopg2.psycopg1 import connect as _pgconnect
    # psycopg2.psycopg1.cursor is too backwards compatible,
    # to the point of avoiding optimized access.
    # only backwards compat thing we need is dict* methods

    import psycopg2.extensions, psycopg2.extras
    from psycopg2.extensions import QuotedString

    I_AUTOCOMMIT = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
    I_READ_COMMITTED = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
    I_REPEATABLE_READ = psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ
    I_SERIALIZABLE = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE

    class _CompatRow(psycopg2.extras.DictRow):
        """Make DictRow more dict-like."""

        def __setitem__(self, k, v):
            """Allow adding new key-value pairs.

            Such operation adds new field to global _index.
            But that is OK, as .description is unchanged, and access
            to such fields before setting them should raise exception
            anyway.
            """
            if type(k) != int:
                if k not in self._index:
                    self._index[k] = len(self._index)
                k = self._index[k]
                while k >= len(self):
                    self.append(None)
            return list.__setitem__(self, k, v)

        def __contains__(self, k):
            """Returns if such row has such column."""
            return k in self._index

        def copy(self):
            """Return regular dict."""
            return dict(self.items())
        
        def iterkeys(self):
            return self._index.iterkeys()

        def itervalues(self):
            return list.__iter__(self)

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
    
    def _pgconnect(cstr):
        """Create a psycopg2 connection."""
        return _CompatConnection(cstr)

except ImportError:
    # use psycopg 1
    I_AUTOCOMMIT = 0
    I_READ_COMMITTED = 1
    I_REPEATABLE_READ = 2
    I_SERIALIZABLE = 2
    try:
        from psycopg import connect as _pgconnect
        from psycopg import QuotedString
    except ImportError:
        print "Please install psycopg2 module"
        sys.exit(1)

def connect_database(connstr):
    """Create a db connection with connect_timeout option.
    
    Default connect_timeout is 15, to change put it directly into dsn.
    """

    # allow override
    if connstr.find("connect_timeout") < 0:
        connstr += " connect_timeout=15"

    # create connection
    db = _pgconnect(connstr)

    # fill .server_version on older psycopg
    if not hasattr(db, 'server_version'):
        iso = db.isolation_level
        db.set_isolation_level(0)
        curs = db.cursor()
        curs.execute('show server_version_num')
        db.server_version = int(curs.fetchone()[0])
        db.set_isolation_level(iso)

    return db

