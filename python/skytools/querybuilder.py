#! /usr/bin/env python

"""Helper classes for complex query generation.

Main target is code execution under PL/Python.

Query parameters are referenced as C{{key}} or C{{key:type}}.
Type will be given to C{plpy.prepare}.
If C{type} is missing, C{text} is assumed.

See L{plpy_exec} for examples.

"""

import skytools

__all__ = [ 
    'QueryBuilder', 'PLPyQueryBuilder', 'PLPyQuery', 'plpy_exec',
    "run_query", "run_query_row", "run_lookup", "run_exists",
]

# make plpy available
try:
    import plpy
except ImportError:
    pass


PARAM_INLINE = 0 # quote_literal()
PARAM_DBAPI = 1  # %()s
PARAM_PLPY = 2   # $n


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
        self.plan_map[t] = pc

        # remove plans if too much
        while len(self.plan_map) > self.maxplans:
            pc = self.plan_list.pop()
            del self.plan_map[pc.key]

        return plan


class QueryBuilder:
    """Helper for query building.

    >>> args = {'success': 't', 'total': 45, 'ccy': 'EEK', 'id': 556}
    >>> q = QueryBuilder("update orders set total = {total} where id = {id}", args)
    >>> q.add(" and optional = {non_exist}")
    >>> q.add(" and final = {success}")
    >>> print q.get_sql(PARAM_INLINE)
    update orders set total = '45' where id = '556' and final = 't'
    >>> print q.get_sql(PARAM_DBAPI)
    update orders set total = %s where id = %s and final = %s
    >>> print q.get_sql(PARAM_PLPY)
    update orders set total = $1 where id = $2 and final = $3
    """

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
            # split name from type
            tpos = k.rfind(':')
            if tpos > 0:
                kparam = k[:tpos]
                ktype = k[tpos+1 : ]
            else:
                kparam = k
                ktype = type

            # params==None means params are checked later
            if params is not None and kparam not in params:
                if required:
                    raise Exception("required parameter missing: "+kparam)
                # optional fragment, param missing, skip it
                return

            # got arg
            nargs += 1
            if params is not None:
                val = params[kparam]
            else:
                val = kparam
            values.append(val)
            types.append(ktype)
            arg = QArg(kparam, val, nargs, self._arg_conf)
            parts.append(arg)

        # add interesting parts to the main sql
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

        if plan_cache is not None:
            if 'plan_cache' not in plan_cache:
                plan_cache['plan_cache'] = PlanCache()
            self._plan_cache = plan_cache['plan_cache']
        else:
            self._plan_cache = None

    def execute(self):
        """Server-side query execution via plpy.

        Query can be run either cached or uncached, depending
        on C{plan_cache} setting given to L{__init__}.

        Returns result of plpy.execute().
        """

        args = self._arg_value_list
        types = self._arg_type_list

        if self._sqls is not None:
            self._sqls.append( { "sql": self.get_sql(PARAM_INLINE) } )

        if self._plan_cache is not None:
            sql = self.get_sql(PARAM_PLPY)
            plan = self._plan_cache.get_plan(sql, types)
            res = plpy.execute(plan, args)
        else:
            sql = self.get_sql(PARAM_INLINE)
            res = plpy.execute(sql)
        if res:
            res = [skytools.dbdict(r) for r in res]
        return res


class PLPyQuery:
    """Static, cached PL/Python query that uses QueryBuilder formatting.
    
    See L{plpy_exec} for simple usage.
    """
    def __init__(self, sql):
        qb = QueryBuilder(sql, None)
        p_sql = qb.get_sql(PARAM_PLPY)
        p_types =  qb._arg_type_list
        self.plan = plpy.prepare(p_sql, p_types)
        self.arg_map = qb._arg_value_list
        self.sql = sql

    def execute(self, arg_dict, all_keys_required = True):
        try:
            if all_keys_required:
                arg_list = [arg_dict[k] for k in self.arg_map]
            else:
                arg_list = [arg_dict.get(k) for k in self.arg_map]
            return plpy.execute(self.plan, arg_list)
        except KeyError:
            need = set(self.arg_map)
            got = set(arg_dict.keys())
            missing = list(need.difference(got))
            plpy.error("Missing arguments: [%s]  QUERY: %s" % (
                ','.join(missing), repr(self.sql)))

    def __repr__(self):
        return 'PLPyQuery<%s>' % self.sql

def plpy_exec(gd, sql, args, all_keys_required = True):
    """Cached plan execution for PL/Python.

    @param gd:  dict to store cached plans under.  If None, caching is disabled.
    @param sql: SQL statement to execute.
    @param args: dict of arguments to query.
    @param all_keys_required: if False, missing key is taken as NULL, instead of throwing error.

    >>> res = plpy_exec(GD, "select {arg1}, {arg2:int4}, {arg1}", {'arg1': '1', 'arg2': '2'})
    DBG: plpy.prepare('select $1, $2, $3', ['text', 'int4', 'text'])
    DBG: plpy.execute(('PLAN', 'select $1, $2, $3', ['text', 'int4', 'text']), ['1', '2', '1'])
    >>> res = plpy_exec(None, "select {arg1}, {arg2:int4}, {arg1}", {'arg1': '1', 'arg2': '2'})
    DBG: plpy.execute("select '1', '2', '1'", [])
    >>> res = plpy_exec(GD, "select {arg1}, {arg2:int4}, {arg1}", {'arg1': '3', 'arg2': '4'})
    DBG: plpy.execute(('PLAN', 'select $1, $2, $3', ['text', 'int4', 'text']), ['3', '4', '3'])
    >>> res = plpy_exec(GD, "select {arg1}, {arg2:int4}, {arg1}", {'arg1': '3'})
    DBG: plpy.error("Missing arguments: [arg2]  QUERY: 'select {arg1}, {arg2:int4}, {arg1}'")
    >>> res = plpy_exec(GD, "select {arg1}, {arg2:int4}, {arg1}", {'arg1': '3'}, False)
    DBG: plpy.execute(('PLAN', 'select $1, $2, $3', ['text', 'int4', 'text']), ['3', None, '3'])
    """

    if gd is None:
        return PLPyQueryBuilder(sql, args).execute()

    try:
        sq = gd['plq_cache'][sql]
    except KeyError:
        if 'plq_cache' not in gd:
            gd['plq_cache'] = {}
        sq = PLPyQuery(sql)
        gd['plq_cache'][sql] = sq
    return sq.execute(args, all_keys_required)

# some helper functions for convenient sql execution

def run_query(cur, sql, params = None, **kwargs):
    """ Helper function if everything you need is just paramertisized execute
        Sets rows_found that is coneninet to use when you don't need result just
        want to know how many rows were affected
    """
    params = params or kwargs
    sql = QueryBuilder(sql, params).get_sql(0)
    cur.execute(sql)
    rows = cur.fetchall()
    # convert result rows to dbdict
    if rows:
        rows = [skytools.dbdict(r) for r in rows]
    return rows

def run_query_row(cur, sql, params = None, **kwargs):
    """ Helper function if everything you need is just paramertisized execute to
        fetch one row only. If not found none is returned
    """
    params = params or kwargs
    rows = run_query(cur, sql, params)
    if len(rows) == 0:
        return None
    return rows[0]

def run_lookup(cur, sql, params = None, **kwargs):
    """ Helper function to fetch one value Takes away all the hassle of preparing statements
        and processing returned result giving out just one value.
    """
    params = params or kwargs
    sql = QueryBuilder(sql, params).get_sql(0)
    cur.execute(sql)
    row = cur.fetchone()
    if row is None:
        return None
    return row[0]

def run_exists(cur, sql, params = None, **kwargs):
    """ Helper function to fetch one value Takes away all the hassle of preparing statements
        and processing returned result giving out just one value.
    """
    params = params or kwargs
    val = run_lookup(cur, sql, params)
    return not (val is None)


# fake plpy for testing
class fake_plpy:
    def prepare(self, sql, types):
        print "DBG: plpy.prepare(%s, %s)" % (repr(sql), repr(types))
        return ('PLAN', sql, types)
    def execute(self, plan, args = []):
        print "DBG: plpy.execute(%s, %s)" % (repr(plan), repr(args))
    def error(self, msg):
        print "DBG: plpy.error(%s)" % repr(msg)

# launch doctest
if __name__ == '__main__':
    import doctest
    plpy = fake_plpy()
    GD = {}
    doctest.testmod()


