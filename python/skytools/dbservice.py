#! /usr/bin/env python

""" Class used to handle multiset receiving and returning PL/Python procedures
"""

import re, skytools

from skytools import dbdict

__all__ = ['DBService', 'ServiceContext',
    'get_record', 'get_record_list',
    'make_record', 'make_record_array',
    'TableAPI',
    #'log_result', 'transform_fields'
]

try:
    import plpy
except ImportError:
    pass

def transform_fields(rows, key_fields, name_field, data_field):
    """Convert multiple-rows per key input array
    to one-row, multiple-column output array.  The input arrays
    must be sorted by the key fields.

    >>> rows = []
    >>> rows.append({'time': '22:00', 'metric': 'count', 'value': 100})
    >>> rows.append({'time': '22:00', 'metric': 'dur', 'value': 7})
    >>> rows.append({'time': '23:00', 'metric': 'count', 'value': 200})
    >>> rows.append({'time': '23:00', 'metric': 'dur', 'value': 5})
    >>> transform_fields(rows, ['time'], 'metric', 'value')
    [{'count': 100, 'dur': 7, 'time': '22:00'}, {'count': 200, 'dur': 5, 'time': '23:00'}]
    """
    cur_key = None
    cur_row = None
    res = []
    for r in rows:
        k = [r[f] for f in key_fields]
        if k != cur_key:
            cur_key = k
            cur_row = {}
            for f in key_fields:
                cur_row[f] = r[f]
            res.append(cur_row)
        cur_row[r[name_field]] = r[data_field]
    return res

# render_table
def render_table(rows, fields):
    """ Render result rows as a table.
        Returns array of lines.
    """
    widths = [15] * len(fields)
    for row in rows:
        for i, k in enumerate(fields):
            rlen = len(str(row.get(k)))
            widths[i] = widths[i] > rlen and widths[i] or rlen
    widths = [w + 2 for w in widths]

    fmt = '%%-%ds' * (len(widths) - 1) + '%%s'
    fmt = fmt % tuple(widths[:-1])

    lines = []
    lines.append(fmt % tuple(fields))
    lines.append(fmt % tuple(['-'*15] * len(fields)))
    for row in rows:
        lines.append(fmt % tuple([str(row.get(k)) for k in fields]))
    return lines

# data conversion to and from url

def get_record(arg):
    """ Parse data for one urlencoded record.
        Useful for turning incoming serialized data into structure usable for manipulation.
    """
    if not arg:
        return dbdict()

    # allow array of single record
    if arg[0] in ('{', '['):
        lst = skytools.parse_pgarray(arg)
        if len(lst) != 1:
            raise ValueError('get_record() expects exactly 1 row, got %d' % len(lst))
        arg = lst[0]

    # parse record
    return dbdict(skytools.db_urldecode(arg))

def get_record_list(array):
    """ Parse array of urlencoded records.
        Useful for turning incoming serialized data into structure usable for manipulation.
    """
    if array is None:
        return []

    if isinstance(array, list):
        return map(get_record, array)
    else:
        return map(get_record, skytools.parse_pgarray(array))

def get_record_lists(tbl, field):
    """ Create dictionary of lists from given list using field as grouping criteria
        Used for master detail operatons to group detail records according to master id
    """
    dict = dbdict()
    for rec in tbl:
        id = str( rec[field] )
        dict.setdefault( id, [] ).append(rec)
    return dict

def _make_record_convert(row):
    """Converts complex values."""
    d = row.copy()
    for k, v in d.items():
        if isinstance(v, list):
            d[k] = skytools.make_pgarray(v)
    return skytools.db_urlencode(d)

def make_record(row):
    """ Takes record as dict and returns it as urlencoded string.
        Used to send data out of db service layer.or to fake incoming calls
    """
    for v in row.values():
        if isinstance(v, list):
            return _make_record_convert(row)
    return skytools.db_urlencode(row)

def make_record_array(rowlist):
    """ Takes list of records got from plpy execute and turns it into postgers aray string.
        Used to send data out of db service layer.
    """
    return '{' + ','.join( map(make_record, rowlist) ) +  '}'

def get_result_items(list, name):
    """ Get return values from result
    """
    for r in list:
        if r['res_code'] == name:
            return get_record_list(r['res_rows'])
    return None

def log_result(log, list):
    """ Sends dbservice execution logs to logfile
    """
    msglist = get_result_items(list, "_status")
    if msglist is None:
        if list:
            log.warning('Unhandled output result: _status res_code not present.')
    else:
        for msg in msglist:
            log.debug( msg['_message'] )


class DBService:
    """  Wrap parameterized query handling and multiset stored procedure writing
    """
    ROW = "_row"            # name of the fake field where internal record id is stored
    FIELD = "_field"        # parameter name for the field in record that is related to current message
    PARAM = "_param"        # name of the parameter to which message relates
    SKIP = "skip"           # used when record is needed for it's data but is not been updated
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    INFO = "info"           # just informative message for the user
    NOTICE = "notice"       # more than info less than warning
    WARNING = "warning"     # warning message, something is out of ordinary
    ERROR = "error"         # error found but execution continues until check then error is raised
    FATAL = "fatal"         # execution is terminated at once and all found errors returned

    def __init__(self, context, global_dict = None):
        """ This object must be initiated in the beginning of each db service
        """
        rec = skytools.db_urldecode(context)
        self._context = context             # used to run dbservice in retval
        self.global_dict = global_dict      # used for cacheing query plans
        self._retval = []                   # used to collect return resultsets
        self._is_test = 'is_test' in rec    # used to convert output into human readable form

        self.sqls = None                    # if sqls stays None then no recording of sqls is done
        if "show_sql" in rec:               # api must add exected sql to resultset
            self.sqls = []                  # sql's executed by dbservice, used for dubugging

        self.can_save = True                # used to keep value most severe error found so far
        self.messages = []                  # used to hold list of messages to be returned to the user

    # error and message handling

    def tell_user(self, severity, code, message, params = None, **kvargs):
        """ Adds another message to the set of messages to be sent back to user
            If error message then can_save is set false
            If fatal message then error or found errors are raised at once
        """
        params = params or kvargs
        #plpy.notice("%s %s: %s %s" % (severity, code, message, str(params)))
        params["_severity"] = severity
        params["_code"] = code
        params["_message"] = message
        self.messages.append( params )
        if severity == self.ERROR:
            self.can_save = False
        if severity == self.FATAL:
            self.can_save = False
            self.raise_if_errors()

    def raise_if_errors(self):
        """ To be used in places where before continuing must be chcked if errors have been found
            Raises found errors packing them into error message as urlencoded string
        """
        if not self.can_save:
            msgs = "Dbservice error(s): " + make_record_array( self.messages )
            plpy.error( msgs )

    # run sql meant mostly for select but not limited to

    def create_query(self, sql, params = None, **kvargs):
        """ Returns initialized querybuilder object for building complex dynamic queries
        """
        params = params or kvargs
        return skytools.PLPyQueryBuilder(sql, params, self.global_dict, self.sqls )

    def run_query(self, sql, params = None, **kvargs):
        """ Helper function if everything you need is just paramertisized execute
            Sets rows_found that is coneninet to use when you don't need result just
            want to know how many rows were affected
        """
        params = params or kvargs
        rows = skytools.plpy_exec(self.global_dict, sql, params)
        # convert result rows to dbdict
        if rows:
            rows = [dbdict(r) for r in rows]
            self.rows_found = len(rows)
        else:
            self.rows_found = 0
        return rows

    def run_query_row(self, sql, params = None, **kvargs):
        """ Helper function if everything you need is just paramertisized execute to
            fetch one row only. If not found none is returned
        """
        params = params or kvargs
        rows = self.run_query( sql, params )
        if len(rows) == 0:
            return None
        return rows[0]

    def run_exists(self, sql, params = None, **kvargs):
        """ Helper function to find out that record in given table exists using
            values in dict as criteria. Takes away all the hassle of preparing statements
            and processing returned result giving out just one boolean
        """
        params = params or kvargs
        self.run_query( sql, params )
        return self.rows_found

    def run_lookup(self, sql, params = None, **kvargs):
        """ Helper function to fetch one value Takes away all the hassle of preparing statements
            and processing returned result giving out just one value. Uses plan cache if used inside
            db service
        """
        params = params or kvargs
        rows = self.run_query( sql, params )
        if len(rows) == 0:
            return None
        row = rows[0]
        return row.values()[0]

     # resultset handling

    def return_next(self, rows, res_name, severity = None):
        """ Adds given set of rows to resultset
        """
        self._retval.append([res_name, rows])
        if severity is not None and len(rows) == 0:
            self.tell_user(severity, "dbsXXXX", "No matching records found")
        return rows

    def return_next_sql(self, sql, params, res_name, severity = None):
        """ Exectes query and adds recors resultset
        """
        rows = self.run_query( sql, params )
        return self.return_next( rows, res_name, severity )

    def retval(self, service_name = None, params = None, **kvargs):
        """ Return collected resultsets and append to the end messages to the users
            Method is called usually as last statment in dbservice to return the results
            Also converts results into desired format
        """
        params = params or kvargs
        self.raise_if_errors()
        if len( self.messages ):
            self.return_next( self.messages, "_status" )
        if self.sqls is not None and len( self.sqls ):
            self.return_next( self.sqls, "_sql" )
        results = []
        for r in self._retval:
            res_name = r[0]
            rows = r[1]
            res_count = str(len(rows))
            if self._is_test and len(rows) > 0:
                results.append([res_name, res_count, res_name])
                n = 1
                for trow in render_table(rows, rows[0].keys()):
                    results.append([res_name, n, trow])
                    n += 1
            else:
                res_rows = make_record_array(rows)
                results.append([res_name, res_count, res_rows])
        if service_name:
            sql = "select * from %s( {i_context}, {i_params} );" % skytools.quote_fqident(service_name)
            par = dbdict( i_context = self._context, i_params = make_record(params) )
            res = self.run_query( sql, par )
            for r in res:
                results.append((r.res_code, r.res_text, r.res_rows))
        return results

    # miscellaneous

    def check_required(self, record_name, record, severity, *fields):
        """ Checks if all required fields are present in record
            Used to validate incoming data
            Returns list of field names that are missing or empty
        """
        missing = []
        params = {self.PARAM: record_name}
        if self.ROW in record:
            params[self.ROW] = record[self.ROW]
        for field in fields:
            params[self.FIELD] = field
            if field in record:
                if record[field] is None or (isinstance(record[field], basestring) and len(record[field]) == 0):
                    self.tell_user(severity, "dbsXXXX", "Required value missing: {%s}.{%s}" % (self.PARAM, self.FIELD), **params)
                    missing.append(field)
            else:
                self.tell_user(severity, "dbsXXXX", "Required field missing: {%s}.{%s}" % (self.PARAM, self.FIELD), **params)
                missing.append(field)
        return missing




# TableAPI
class TableAPI:
    """ Class for managing one record updates using primary key
    """
    _table = None   # schema name and table name
    _where = None   # where condition used for update and delete
    _id = None      # name of the primary key filed
    _id_type = None # column type of primary key
    _op = None      # operation currently carried out
    _ctx = None     # context object for username and version
    _logging = True # should tapi log data changed
    _row = None     # row identifer from calling program

    def __init__(self, ctx, table, create_log = True, id_type='int8' ):
        """ Table name is used to construct insert update and delete statements
            Table must have primary key field whose name is in format id_<table>
            Tablename should be in format schema.tablename
        """
        self._ctx = ctx
        self._table = skytools.quote_fqident(table)
        self._id = "id_" + skytools.fq_name_parts(table)[1]
        self._id_type = id_type
        self._where = '%s = {%s:%s}' % (skytools.quote_ident(self._id), self._id, self._id_type)
        self._logging = create_log

    def _log(self, result, original = None):
        """ Log changei into table log.changelog
        """
        if not self._logging:
            return
        changes = []
        for key in result.keys():
            if self._op == 'update':
                if key in original:
                    if str(original[key]) <> str(result[key]):
                        changes.append( key + ": " + str(original[key]) + " -> " + str(result[key]) )
            else:
                changes.append( key + ": " + str(result[key]) )
        self._ctx.log( self._table,  result[ self._id ], self._op, "\n".join(changes) )

    def _version_check(self, original, version):
        if original is None:
            self._ctx.tell_user( self._ctx.INFO, "dbsXXXX",
                "Record ({table}.{field}={id}) has been deleted by other user while you were editing. Check version ({ver}) in changelog for details.",
                table = self._table, field = self._id, id = original[self._id], ver = original.version, _row = self._row )
        if version is not None and original.version is not None:
            if int(version) != int(original.version):
                    self._ctx.tell_user( self._ctx.INFO, "dbsXXXX",
                            "Record ({table}.{field}={id}) has been changed by other user while you were editing. Version in db: ({db_ver}) and version sent by caller ({caller_ver}). See changelog for details.",
                        table = self._table, field = self._id, id = original[self._id], db_ver = original.version, caller_ver = version, _row = self._row )

    def _insert(self, data):
        fields = []
        values = []
        for key in data.keys():
            if data[key] is not None:       # ignore empty
                fields.append(skytools.quote_ident(key))
                values.append("{" + key + "}")
        sql = "insert into %s (%s) values (%s) returning *;" % ( self._table, ",".join(fields), ",".join(values))
        result = self._ctx.run_query_row( sql, data )
        self._log( result )
        return result

    def _update(self, data, version):
        sql = "select * from %s where %s" % ( self._table, self._where )
        original = self._ctx.run_query_row( sql, data )
        self._version_check( original, version )
        pairs = []
        for key in data.keys():
            if data[key] is None:
                pairs.append( key + " = NULL" )
            else:
                pairs.append( key + " = {" + key + "}" )
        sql = "update %s set %s where %s returning *;" % ( self._table, ", ".join(pairs), self._where )
        result = self._ctx.run_query_row( sql, data )
        self._log( result, original )
        return result

    def _delete(self, data, version):
        sql = "delete from %s where %s returning *;" % ( self._table, self._where )
        result = self._ctx.run_query_row( sql, data )
        self._version_check( result, version )
        self._log( result )
        return result

    def do(self, data):
        """ Do dml according to special field _op that must be given together wit data
        """
        result = data                               # so it is initialized for skip
        self._op = data.pop(self._ctx.OP)           # determines operation done
        self._row = data.pop(self._ctx.ROW, None)   # internal record id used for error reporting
        if self._row is None:                       # if no _row variable was provided
            self._row = data.get(self._id, None)    # use id instead
        if self._id in data and data[self._id]:     # if _id field is given
            if int( data[self._id] ) < 0:           # and it is fake key generated by ui
                data.pop(self._id)                  # remove fake key so real one can be assigned
        version = data.get('version', None)         # version sent from caller
        data['version'] = self._ctx.version         # current transaction id is stored in each record
        if   self._op == self._ctx.INSERT: result = self._insert( data )
        elif self._op == self._ctx.UPDATE: result = self._update( data, version )
        elif self._op == self._ctx.DELETE: result = self._delete( data, version )
        elif self._op == self._ctx.SKIP:   None
        else:
            self._ctx.tell_user( self._ctx.ERROR, "dbsXXXX",
                "Unahndled _op='{op}' value in TableAPI (table={table}, id={id})",
                op = self._op, table = self._table, id = data[self._id] )
        result[self._ctx.OP] = self._op
        result[self._ctx.ROW] = self._row
        return result

# ServiceContext
class ServiceContext(DBService):
    OP = "_op"              # name of the fake field where record modificaton operation is stored

    def __init__(self, context, global_dict = None):
        """ This object must be initiated in the beginning of each db service
        """
        DBService.__init__(self, context, global_dict)

        rec = skytools.db_urldecode(context)
        if "username" not in rec:
            plpy.error("Username must be provided in db service context parameter")
        self.username = rec['username']     # used for logging purposes

        res = plpy.execute("select txid_current() as txid;")
        row = res[0]
        self.version = row["txid"]
        self.rows_found = 0                 # Flag set by run query to inicate number of rows got

    # logging

    def log(self, _object_type, _key_object, _change_op, _payload):
        """ Log stuff into the changelog whatever seems relevant to be logged
        """
        self.run_query(
            "select log.log_change( {version}, {username}, {object_type}, {key_object}, {change_op}, {payload} );",
                version= self.version , username= self.username ,
                object_type= _object_type , key_object= _key_object ,
                change_op= _change_op , payload= _payload )

    # data conversion to and from url

    def get_record(self, arg):
        """ Parse data for one urlencoded record.
            Useful for turning incoming serialized data into structure usable for manipulation.
        """
        return get_record(arg)

    def get_record_list(self, array):
        """ Parse array of urlencoded records.
            Useful for turning incoming serialized data into structure usable for manipulation.
        """
        return get_record_list(array)

    def get_list_groups(self, tbl, field):
        """ Create dictionary of lists from given list using field as grouping criteria
            Used for master detail operatons to group detail records according to master id
        """
        return get_record_lists(tbl, field)

    def make_record(self, row):
        """ Takes record as dict and returns it as urlencoded string.
            Used to send data out of db service layer.or to fake incoming calls
        """
        return make_record(row)

    def make_record_array(self, rowlist):
        """ Takes list of records got from plpy execute and turns it into postgers aray string.
            Used to send data out of db service layer.
        """
        return make_record_array(rowlist)

    # tapi based dml functions

    def _changelog(self, fields):
        log = True
        if fields:
            if '_log' in fields:
                if not fields.pop('_log'):
                    log = False
            if '_log_id' in fields:
                fields.pop('_log_id')
            if '_log_field' in fields:
                fields.pop('_log_field')
        return log

    def tapi_do(self, tablename, row, **fields):
        """ Convenience function for just doing the change without creating tapi object first
            Fields object may contain aditional overriding values that are aplied before do
        """
        tapi =  TableAPI(self, tablename, self._changelog(fields))
        row = row or dbdict()
        fields and row.update(fields)
        return tapi.do( row )

    def tapi_do_set(self, tablename, rows, **fields):
        """ Does changes to list of detail rows
            Used for normal foreign keys in master detail relationships
            Dows first deletes then updates and then inserts to avoid uniqueness problems
        """
        tapi = TableAPI(self, tablename, self._changelog(fields))
        results, updates, inserts = [], [], []
        for row in rows:
            fields and row.update(fields)
            if row[self.OP] == self.DELETE:
                results.append( tapi.do( row ) )
            elif row[self.OP] == self.UPDATE:
                updates.append( row )
            else:
                inserts.append( row )
        for row in updates:
            results.append( tapi.do( row ) )
        for row in inserts:
            results.append( tapi.do( row ) )
        return results

    # resultset handling

    def retval_dbservice(self, service_name, ctx, **params):
        """ Runs service with standard interface.
            Convenient to use for calling select services from other services
            For example to return data after doing save
        """
        self.raise_if_errors()
        service_sql = "select * from %s( {i_context}, {i_params} );" % skytools.quote_fqident(service_name)
        service_params = { "i_context": ctx, "i_params": self.make_record(params) }
        results = self.run_query( service_sql, service_params )
        retval = self.retval()
        for r in results:
            retval.append((r.res_code, r.res_text, r.res_rows))
        return retval

    # miscellaneous

    def field_copy(self, dict, *keys):
        """ Used to copy subset of fields from one record into another
            example: dbs.copy(record, hosting) "start_date", "key_colo", "key_rack")
        """
        retval = dbdict()
        for key in keys:
            if key in dict:
                retval[key] = dict[key]
        return retval

    def field_set(self, **fields):
        """ Fills dict with given values and returns resulting dict
            If dict was not provied with call it is created
        """
        return fields
