
"""Wrapper around psycopg2.

Database connection provides regular DB-API 2.0 interface.

Connection object methods::

    .cursor()

    .commit()

    .rollback()

    .close()

Cursor methods::

    .execute(query[, args])

    .fetchone()

    .fetchall()


Sample usage::

    db = self.get_database('somedb')
    curs = db.cursor()

    # query arguments as array
    q = "select * from table where id = %s and name = %s"
    curs.execute(q, [1, 'somename'])

    # query arguments as dict
    q = "select id, name from table where id = %(id)s and name = %(name)s"
    curs.execute(q, {'id': 1, 'name': 'somename'})

    # loop over resultset
    for row in curs.fetchall():

        # columns can be asked by index:
        id = row[0]
        name = row[1]

        # and by name:
        id = row['id']
        name = row['name']

    # now commit the transaction
    db.commit()

Deprecated interface:  .dictfetchall/.dictfetchone functions on cursor.
Plain .fetchall() / .fetchone() give exact same result.

"""

__all__ = ['connect_database', 'DBError', 'I_AUTOCOMMIT', 'I_READ_COMMITTED',
           'I_REPEATABLE_READ', 'I_SERIALIZABLE']

import sys
import socket
import psycopg2.extensions
import psycopg2.extras
import skytools

from psycopg2 import Error as DBError
from skytools.sockutil import set_tcp_keepalive


I_AUTOCOMMIT = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
I_READ_COMMITTED = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
I_REPEATABLE_READ = psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ
I_SERIALIZABLE = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE


class _CompatRow(psycopg2.extras.DictRow):
    """Make DictRow more dict-like."""
    __slots__ = ('_index',)

    def __contains__(self, k):
        """Returns if such row has such column."""
        return k in self._index

    def copy(self):
        """Return regular dict."""
        return skytools.dbdict(self.iteritems())
    
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
    my_name = '?'
    def cursor(self, name = None):
        if name:
            return psycopg2.extensions.connection.cursor(self,
                    cursor_factory = _CompatCursor,
                    name = name)
        else:
            return psycopg2.extensions.connection.cursor(self,
                    cursor_factory = _CompatCursor)

def connect_database(connstr, keepalive = True,
                     tcp_keepidle = 4 * 60,     # 7200
                     tcp_keepcnt = 4,           # 9
                     tcp_keepintvl = 15):       # 75
    """Create a db connection with connect_timeout and TCP keepalive.
    
    Default connect_timeout is 15, to change put it directly into dsn.

    The extra tcp_* options are Linux-specific, see `man 7 tcp` for details.
    """

    # allow override
    if connstr.find("connect_timeout") < 0:
        connstr += " connect_timeout=15"

    # create connection
    db = _CompatConnection(connstr)
    curs = db.cursor()

    # tune keepalive
    fd = hasattr(db, 'fileno') and db.fileno() or curs.fileno()
    set_tcp_keepalive(fd, keepalive, tcp_keepidle, tcp_keepcnt, tcp_keepintvl)

    # fill .server_version on older psycopg
    if not hasattr(db, 'server_version'):
        iso = db.isolation_level
        db.set_isolation_level(0)
        curs.execute('show server_version_num')
        db.server_version = int(curs.fetchone()[0])
        db.set_isolation_level(iso)

    return db

