
"""Database tools."""

import os
from cStringIO import StringIO
from skytools.quoting import quote_copy, quote_literal, quote_ident, quote_fqident
import skytools.installer_config
try:
    import plpy
except ImportError:
    pass

__all__ = [
    "fq_name_parts", "fq_name", "get_table_oid", "get_table_pkeys",
    "get_table_columns", "exists_schema", "exists_table", "exists_type",
    "exists_sequence", "exists_temp_table",
    "exists_function", "exists_language", "Snapshot", "magic_insert",
    "CopyPipe", "full_copy", "DBObject", "DBSchema", "DBTable", "DBFunction",
    "DBLanguage", "db_install", "installer_find_file", "installer_apply_file",
    "dbdict", "mk_insert_sql", "mk_update_sql", "mk_delete_sql",
    'QueryBuilder', 'PLPyQueryBuilder',
]

PARAM_INLINE = 0 # quote_literal()
PARAM_DBAPI = 1  # %()s
PARAM_PLPY = 2   # $n

class dbdict(dict):
    """Wrapper on actual dict that allows
    accessing dict keys as attributes."""
    # obj.foo access
    def __getattr__(self, k):
        "Return attribute."
        return self[k]
    def __setattr__(self, k, v):
        "Set attribute."
        self[k] = v
    def __delattr__(self, k):
        "Remove attribute"
        del self[k]

#
# Fully qualified table name
#

def fq_name_parts(tbl):
    "Return fully qualified name parts."

    tmp = tbl.split('.', 1)
    if len(tmp) == 1:
        return ('public', tbl)
    elif len(tmp) == 2:
        return tmp
    else:
        raise Exception('Syntax error in table name:'+tbl)

def fq_name(tbl):
    "Return fully qualified name."
    return '.'.join(fq_name_parts(tbl))

#
# info about table
#
def get_table_oid(curs, table_name):
    """Find Postgres OID for table."""
    schema, name = fq_name_parts(table_name)
    q = """select c.oid from pg_namespace n, pg_class c
           where c.relnamespace = n.oid
             and n.nspname = %s and c.relname = %s"""
    curs.execute(q, [schema, name])
    res = curs.fetchall()
    if len(res) == 0:
        raise Exception('Table not found: '+table_name)
    return res[0][0]

def get_table_pkeys(curs, tbl):
    """Return list of pkey column names."""
    oid = get_table_oid(curs, tbl)
    q = "SELECT k.attname FROM pg_index i, pg_attribute k"\
        " WHERE i.indrelid = %s AND k.attrelid = i.indexrelid"\
        "   AND i.indisprimary AND k.attnum > 0 AND NOT k.attisdropped"\
        " ORDER BY k.attnum"
    curs.execute(q, [oid])
    return map(lambda x: x[0], curs.fetchall())

def get_table_columns(curs, tbl):
    """Return list of column names for table."""
    oid = get_table_oid(curs, tbl)
    q = "SELECT k.attname FROM pg_attribute k"\
        " WHERE k.attrelid = %s"\
        "   AND k.attnum > 0 AND NOT k.attisdropped"\
        " ORDER BY k.attnum"
    curs.execute(q, [oid])
    return map(lambda x: x[0], curs.fetchall())

#
# exist checks
#
def exists_schema(curs, schema):
    """Does schema exists?"""
    q = "select count(1) from pg_namespace where nspname = %s"
    curs.execute(q, [schema])
    res = curs.fetchone()
    return res[0]

def exists_table(curs, table_name):
    """Does table exists?"""
    schema, name = fq_name_parts(table_name)
    q = """select count(1) from pg_namespace n, pg_class c
           where c.relnamespace = n.oid and c.relkind = 'r'
             and n.nspname = %s and c.relname = %s"""
    curs.execute(q, [schema, name])
    res = curs.fetchone()
    return res[0]

def exists_sequence(curs, seq_name):
    """Does sequence exists?"""
    schema, name = fq_name_parts(seq_name)
    q = """select count(1) from pg_namespace n, pg_class c
           where c.relnamespace = n.oid and c.relkind = 'S'
             and n.nspname = %s and c.relname = %s"""
    curs.execute(q, [schema, name])
    res = curs.fetchone()
    return res[0]

def exists_type(curs, type_name):
    """Does type exists?"""
    schema, name = fq_name_parts(type_name)
    q = """select count(1) from pg_namespace n, pg_type t
           where t.typnamespace = n.oid
             and n.nspname = %s and t.typname = %s"""
    curs.execute(q, [schema, name])
    res = curs.fetchone()
    return res[0]

def exists_function(curs, function_name, nargs):
    """Does function exists?"""
    # this does not check arg types, so may match several functions
    schema, name = fq_name_parts(function_name)
    q = """select count(1) from pg_namespace n, pg_proc p
           where p.pronamespace = n.oid and p.pronargs = %s
             and n.nspname = %s and p.proname = %s"""
    curs.execute(q, [nargs, schema, name])
    res = curs.fetchone()

    # if unqualified function, check builtin functions too
    if not res[0] and function_name.find('.') < 0:
        name = "pg_catalog." + function_name
        return exists_function(curs, name, nargs)

    return res[0]

def exists_language(curs, lang_name):
    """Does PL exists?"""
    q = """select count(1) from pg_language
           where lanname = %s"""
    curs.execute(q, [lang_name])
    res = curs.fetchone()
    return res[0]

def exists_temp_table(curs, tbl):
    """Does temp table exists?"""
    # correct way, works only on 8.2
    q = "select 1 from pg_class where relname = %s and relnamespace = pg_my_temp_schema()"
    curs.execute(q, [tbl])
    tmp = curs.fetchall()
    return len(tmp) > 0

#
# Support for PostgreSQL snapshot
#

class Snapshot(object):
    "Represents a PostgreSQL snapshot."

    def __init__(self, str):
        "Create snapshot from string."

        self.sn_str = str
        tmp = str.split(':')
        if len(tmp) != 3:
            raise Exception('Unknown format for snapshot')
        self.xmin = int(tmp[0])
        self.xmax = int(tmp[1])
        self.txid_list = []
        if tmp[2] != "":
            for s in tmp[2].split(','):
                self.txid_list.append(int(s))

    def contains(self, txid):
        "Is txid visible in snapshot."

        txid = int(txid)

        if txid < self.xmin:
            return True
        if txid >= self.xmax:
            return False
        if txid in self.txid_list:
            return False
        return True

#
# Copy helpers
#

def _gen_dict_copy(tbl, row, fields, qfields):
    tmp = []
    for f in fields:
        v = row.get(f)
        tmp.append(quote_copy(v))
    return "\t".join(tmp)

def _gen_dict_insert(tbl, row, fields, qfields):
    tmp = []
    for f in fields:
        v = row.get(f)
        tmp.append(quote_literal(v))
    fmt = "insert into %s (%s) values (%s);"
    return fmt % (tbl, ",".join(qfields), ",".join(tmp))

def _gen_list_copy(tbl, row, fields, qfields):
    tmp = []
    for i in range(len(fields)):
        v = row[i]
        tmp.append(quote_copy(v))
    return "\t".join(tmp)

def _gen_list_insert(tbl, row, fields, qfields):
    tmp = []
    for i in range(len(fields)):
        v = row[i]
        tmp.append(quote_literal(v))
    fmt = "insert into %s (%s) values (%s);"
    return fmt % (tbl, ",".join(qfields), ",".join(tmp))

def magic_insert(curs, tablename, data, fields = None, use_insert = 0):
    """Copy/insert a list of dict/list data to database.
    
    If curs == None, then the copy or insert statements are returned
    as string.  For list of dict the field list is optional, as its
    possible to guess them from dict keys.
    """
    if len(data) == 0:
        return

    # decide how to process
    if hasattr(data[0], 'keys'):
        if fields == None:
            fields = data[0].keys()
        if use_insert:
            row_func = _gen_dict_insert
        else:
            row_func = _gen_dict_copy
    else:
        if fields == None:
            raise Exception("Non-dict data needs field list")
        if use_insert:
            row_func = _gen_list_insert
        else:
            row_func = _gen_list_copy

    qfields = [quote_ident(f) for f in fields]
    qtablename = quote_fqident(tablename)

    # init processing
    buf = StringIO()
    if curs == None and use_insert == 0:
        fmt = "COPY %s (%s) FROM STDIN;\n"
        buf.write(fmt % (qtablename, ",".join(qfields)))
 
    # process data
    for row in data:
        buf.write(row_func(qtablename, row, fields, qfields))
        buf.write("\n")

    # if user needs only string, return it
    if curs == None:
        if use_insert == 0:
            buf.write("\\.\n")
        return buf.getvalue()

    # do the actual copy/inserts
    if use_insert:
        curs.execute(buf.getvalue())
    else:
        buf.seek(0)
        hdr = "%s (%s)" % (qtablename, ",".join(qfields))
        curs.copy_from(buf, hdr)

#
# Full COPY of table from one db to another
#

class CopyPipe(object):
    "Splits one big COPY to chunks."

    def __init__(self, dstcurs, tablename = None, limit = 512*1024, cancel_func=None, sql_from = None):
        self.tablename = tablename
        self.sql_from = sql_from
        self.dstcurs = dstcurs
        self.buf = StringIO()
        self.limit = limit
        self.cancel_func = None
        self.total_rows = 0
        self.total_bytes = 0

    def write(self, data):
        "New data from psycopg"

        self.total_bytes += len(data)
        self.total_rows += data.count("\n")

        if self.buf.tell() >= self.limit:
            pos = data.find('\n')
            if pos >= 0:
                # split at newline
                p1 = data[:pos + 1]
                p2 = data[pos + 1:]
                self.buf.write(p1)
                self.flush()

                data = p2

        self.buf.write(data)

    def flush(self):
        "Send data out."

        if self.cancel_func:
            self.cancel_func()

        if self.buf.tell() <= 0:
            return

        self.buf.seek(0)
        if self.sql_from:
            self.dstcurs.copy_expert(self.sql_from, self.buf)
        else:
            self.dstcurs.copy_from(self.buf, self.tablename)
        self.buf.seek(0)
        self.buf.truncate()

def full_copy(tablename, src_curs, dst_curs, column_list = []):
    """COPY table from one db to another."""

    qtable = quote_fqident(tablename)
    if column_list:
        qfields = [quote_ident(f) for f in column_list]
        hdr = "%s (%s)" % (qtable, ",".join(qfields))
    else:
        hdr = qtable
    if hasattr(src_curs, 'copy_expert'):
        sql_to = "COPY %s TO stdout" % hdr
        sql_from = "COPY %s FROM stdout" % hdr
        buf = CopyPipe(dst_curs, sql_from = sql_from)
        src_curs.copy_expert(sql_to, buf)
    else:
        buf = CopyPipe(dst_curs, hdr)
        src_curs.copy_to(buf, hdr)
    buf.flush()

    return (buf.total_bytes, buf.total_rows)


#
# SQL installer
#

class DBObject(object):
    """Base class for installable DB objects."""
    name = None
    sql = None
    sql_file = None
    def __init__(self, name, sql = None, sql_file = None):
        """Generic dbobject init."""
        self.name = name
        self.sql = sql
        self.sql_file = sql_file

    def create(self, curs, log = None):
        """Create a dbobject."""
        if log:
            log.info('Installing %s' % self.name)
        if self.sql:
            sql = self.sql
        elif self.sql_file:
            fn = self.find_file()
            if log:
                log.info("  Reading from %s" % fn)
            sql = open(fn, "r").read()
        else:
            raise Exception('object not defined')
        for stmt in skytools.parse_statements(sql):
            #if log: log.debug(repr(stmt))
            curs.execute(stmt)

    def find_file(self):
        """Find install script file."""
        full_fn = None
        if self.sql_file[0] == "/":
            full_fn = self.sql_file
        else:
            dir_list = skytools.installer_config.sql_locations
            for fdir in dir_list:
                fn = os.path.join(fdir, self.sql_file)
                if os.path.isfile(fn):
                    full_fn = fn
                    break

        if not full_fn:
            raise Exception('File not found: '+self.sql_file)
        return full_fn

class DBSchema(DBObject):
    """Handles db schema."""
    def exists(self, curs):
        """Does schema exists."""
        return exists_schema(curs, self.name)

class DBTable(DBObject):
    """Handles db table."""
    def exists(self, curs):
        """Does table exists."""
        return exists_table(curs, self.name)

class DBFunction(DBObject):
    """Handles db function."""
    def __init__(self, name, nargs, sql = None, sql_file = None):
        """Function object - number of args is significant."""
        DBObject.__init__(self, name, sql, sql_file)
        self.nargs = nargs
    def exists(self, curs):
        """Does function exists."""
        return exists_function(curs, self.name, self.nargs)

class DBLanguage(DBObject):
    """Handles db language."""
    def __init__(self, name):
        """PL object - creation happens with CREATE LANGUAGE."""
        DBObject.__init__(self, name, sql = "create language %s" % name)
    def exists(self, curs):
        """Does PL exists."""
        return exists_language(curs, self.name)

def db_install(curs, list, log = None):
    """Installs list of objects into db."""
    for obj in list:
        if not obj.exists(curs):
            obj.create(curs, log)
        else:
            if log:
                log.info('%s is installed' % obj.name)

def installer_find_file(filename):
    """Find SQL script from pre-defined paths."""
    full_fn = None
    if filename[0] == "/":
        if os.path.isfile(filename):
            full_fn = filename
    else:
        dir_list = ["."] + skytools.installer_config.sql_locations
        for fdir in dir_list:
            fn = os.path.join(fdir, filename)
            if os.path.isfile(fn):
                full_fn = fn
                break

    if not full_fn:
        raise Exception('File not found: '+filename)
    return full_fn

def installer_apply_file(db, filename, log):
    """Find SQL file and apply it to db, statement-by-statement."""
    fn = installer_find_file(filename)
    sql = open(fn, "r").read()
    if log:
        log.info("applying %s" % fn)
    curs = db.cursor()
    for stmt in skytools.parse_statements(sql):
        #log.debug(repr(stmt))
        curs.execute(stmt)

#
# Generate INSERT/UPDATE/DELETE statement
#

def mk_insert_sql(row, tbl, pkey_list = None, field_map = None):
    """Generate INSERT statement from dict data."""

    col_list = []
    val_list = []
    if field_map:
        for src, dst in field_map.iteritems():
            col_list.append(quote_ident(dst))
            val_list.append(quote_literal(row[src]))
    else:
        for c, v in row.iteritems():
            col_list.append(quote_ident(c))
            val_list.append(quote_literal(v))
    col_str = ", ".join(col_list)
    val_str = ", ".join(val_list)
    return "insert into %s (%s) values (%s);" % (
                    quote_fqident(tbl), col_str, val_str)

def mk_update_sql(row, tbl, pkey_list, field_map = None):
    """Generate UPDATE statement from dict data."""

    if len(pkey_list) < 1:
        raise Exception("update needs pkeys")
    set_list = []
    whe_list = []
    pkmap = {}
    for k in pkey_list:
        pkmap[k] = 1
        new_k = field_map and field_map[k] or k
        col = quote_ident(new_k)
        val = quote_literal(row[k])
        whe_list.append("%s = %s" % (col, val))

    if field_map:
        for src, dst in field_map.iteritems():
            if src not in pkmap:
                col = quote_ident(dst)
                val = quote_literal(row[src])
                set_list.append("%s = %s" % (col, val))
    else:
        for col, val in row.iteritems():
            if col not in pkmap:
                col = quote_ident(col)
                val = quote_literal(val)
                set_list.append("%s = %s" % (col, val))
    return "update only %s set %s where %s;" % (quote_fqident(tbl),
            ", ".join(set_list), " and ".join(whe_list))

def mk_delete_sql(row, tbl, pkey_list, field_map = None):
    """Generate DELETE statement from dict data."""

    if len(pkey_list) < 1:
        raise Exception("delete needs pkeys")
    whe_list = []
    for k in pkey_list:
        new_k = field_map and field_map[k] or k
        col = quote_ident(new_k)
        val = quote_literal(row[k])
        whe_list.append("%s = %s" % (col, val))
    whe_str = " and ".join(whe_list) 
    return "delete from only %s where %s;" % (quote_fqident(tbl), whe_str)

class QArgConf:
    """Per-query arg-type config object."""
    param_type = None

class QArg:
    """Place-holder for a query parameter."""
    def __init__(self, name, value, pos, conf):
        self.name = name
        self.value = value
        self.pos = pos
        self.conf = conf
    def __str__(self):
        if self.conf.param_type == PARAM_INLINE:
            return skytools.quote_literal(self.value)
        elif self.conf.param_type == PARAM_DBAPI:
            return "%s"
        elif self.conf.param_type == PARAM_PLPY:
            return "$%d" % self.pos
        else:
            raise Exception("bad QArgConf.param_type")


# need an structure with fast remove-from-middle
# and append operations.
class DList:
    """Simple double-linked list."""
    def __init__(self):
        self.next = self
        self.prev = self

    def append(self, obj):
        obj.next = self
        obj.prev = self.prev
        self.prev.next = obj
        self.prev = obj

    def remove(self, obj):
        obj.next.prev = obj.prev
        obj.prev.next = obj.next
        obj.next = obj.prev = None

    def empty(self):
        return self.next == self

    def pop(self):
        """Remove and return first element."""
        obj = None
        if not self.empty():
            obj = self.next
            self.remove(obj)
        return obj


class CachedPlan:
    """Wrapper around prepared plan."""
    def __init__(self, key, plan):
        self.key = key # (sql, (types))
        self.plan = plan


class PlanCache:
    """Cache for limited amount of plans."""

    def __init__(self, maxplans = 100):
        self.maxplans = maxplans
        self.plan_map = {}
        self.plan_list = DList()

    def get_plan(self, sql, types):
        """Prepare the plan and cache it."""

        t = (sql, tuple(types))
        if t in self.plan_map:
            pc = self.plan_map[t]
            # put to the end
            self.plan_list.remove(pc)
            self.plan_list.append(pc)
            return pc.plan

        # prepare new plan
        plan = plpy.prepare(sql, types)

        # add to cache
        pc = CachedPlan(t, plan)
        self.plan_list.append(pc)
        self.plan_map[t] = plan

        # remove plans if too much
        while len(self.plan_map) > self.maxplans:
            pc = self.plan_list.pop()
            del self.plan_map[pc.key]

        return plan


class QueryBuilder:
    """Helper for query building."""

    def __init__(self, sqlexpr, params):
        """Init the object.

        @param sqlexpr:     Partial sql fragment.
        @param params:      Dict of parameter values.
        """
        self._params = params
        self._arg_type_list = []
        self._arg_value_list = []
        self._sql_parts = []
        self._arg_conf = QArgConf()
        self._nargs = 0

        if sqlexpr:
            self.add(sqlexpr, required = True)

    def add(self, expr, type = "text", required = False):
        """Add SQL fragment to query.
        """
        self._add_expr('', expr, self._params, type, required)

    def get_sql(self, param_type = PARAM_INLINE):
        """Return generated SQL (thus far) as string.
        
        Possible values for param_type:
            - 0: Insert values quoted with quote_literal()
            - 1: Insert %()s in place of parameters.
            - 2: Insert $n in place of parameters.
        """
        self._arg_conf.param_type = param_type
        tmp = map(str, self._sql_parts)
        return "".join(tmp)

    def _add_expr(self, pfx, expr, params, type, required):
        parts = []
        types = []
        values = []
        nargs = self._nargs
        if pfx:
            parts.append(pfx)
        pos = 0
        while 1:
            # find start of next argument
            a1 = expr.find('{', pos)
            if a1 < 0:
                parts.append(expr[pos:])
                break

            # find end end of argument name
            a2 = expr.find('}', a1)
            if a2 < 0:
                raise Exception("missing argument terminator: "+expr)
            
            # add plain sql
            if a1 > pos:
                parts.append(expr[pos:a1])
            pos = a2 + 1

            # get arg name, check if exists
            k = expr[a1 + 1 : a2]
            if k not in params:
                if required:
                    raise Exception("required parameter missing: "+k)
                return

            # got arg
            nargs += 1
            val = params[k]
            values.append(val)
            types.append(type)
            arg = QArg(k, val, nargs, self._arg_conf)
            parts.append(arg)

        # add to the main sql only if all args exist
        self._sql_parts.extend(parts)
        if types:
            self._arg_type_list.extend(types)
        if values:
            self._arg_value_list.extend(values)
        self._nargs = nargs

    def execute(self, curs):
        """Client-side query execution on DB-API 2.0 cursor.

        Calls C{curs.execute()} with proper arguments.

        Returns result of curs.execute(), although that does not
        return anything interesting.  Later curs.fetch* methods
        must be called to get result.
        """
        q = self.get_sql(PARAM_DBAPI)
        args = self._params
        return curs.execute(q, args)

class PLPyQueryBuilder(QueryBuilder):
    
    def __init__(self, sqlexpr, params, plan_cache = None, sqls = None):
        """Init the object.

        @param sqlexpr:     Partial sql fragment.
        @param params:      Dict of parameter values.
        @param plan_cache:  (PL/Python) A dict object where to store the plan cache, under the key C{"plan_cache"}.
                            If not given, plan will not be cached and values will be inserted directly
                            to query.  Usually either C{GD} or C{SD} should be given here.
        @param sqls:        list object where to append executed sqls (used for debugging)
        """
        QueryBuilder.__init__(self, sqlexpr, params)
        self._sqls = sqls
        
        if plan_cache:
            if 'plan_cache' not in plan_cache:
                plan_cache['plan_cache'] = PlanCache()
            self._plan_cache = plan_cache['plan_cache']
        else:
            self._plan_cache = None

    def execute(self):
        """Server-size query execution via plpy.

        Query can be run either cached or uncached, depending
        on C{plan_cache} setting given to L{__init__}.

        Returns result of plpy.execute().
        """

        args = self._arg_value_list
        types = self._arg_type_list
        
        if self._sqls is not None:
            self._sqls.append( { "sql": self.get_sql(PARAM_INLINE) } )
        
        if self._plan_cache:
            sql = self.get_sql(PARAM_PLPY)
            plan = self._plan_cache.get_plan(sql, types)
            res = plpy.execute(plan, args)
        else:
            sql = self.get_sql(PARAM_INLINE)
            res = plpy.execute(sql)
        if res:
            res = [dbdict(r) for r in res]
        return res

