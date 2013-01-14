
"""Table handler.

Per-table decision how to create trigger, copy data and apply events.
"""

"""
-- redirect & create table
partition by batch_time
partition by date field

-- sql handling:
cube1 - I/U/D -> partition, insert
cube2 - I/U/D -> partition, del/insert
field remap
name remap

bublin filter
- replay: filter events
- copy: additional where
- add: add trigger args

multimaster
- replay: conflict handling, add fncall to sql queue?
- add: add 'backup' arg to trigger

plain londiste:
- replay: add to sql queue

"""

import sys
import logging
import skytools
import londiste.handlers

__all__ = ['RowCache', 'BaseHandler', 'build_handler', 'EncodingValidator',
           'load_handler_modules', 'create_handler_string']

class RowCache:
    def __init__(self, table_name):
        self.table_name = table_name
        self.keys = {}
        self.rows = []

    def add_row(self, d):
        row = [None] * len(self.keys)
        for k, v in d.items():
            try:
                row[self.keys[k]] = v
            except KeyError:
                i = len(row)
                self.keys[k] = i
                row.append(v)
        row = tuple(row)
        self.rows.append(row)

    def get_fields(self):
        row = [None] * len(self.keys)
        for k, i in self.keys.keys():
            row[i] = k
        return tuple(row)

    def apply_rows(self, curs):
        fields = self.get_fields()
        skytools.magic_insert(curs, self.table_name, self.rows, fields)

class BaseHandler:
    """Defines base API, does nothing.
    """
    handler_name = 'nop'
    log = logging.getLogger('basehandler')

    def __init__(self, table_name, args, dest_table):
        self.table_name = table_name
        self.dest_table = dest_table or table_name
        self.fq_table_name = skytools.quote_fqident(self.table_name)
        self.fq_dest_table = skytools.quote_fqident(self.dest_table)
        self.args = args
        self._check_args (args)

    def _parse_args_from_doc (self):
        doc = self.__doc__ or ""
        params_descr = []
        params_found = False
        for line in doc.splitlines():
            ln = line.strip()
            if params_found:
                if ln == "":
                    break
                descr = ln.split (None, 1)
                name, sep, rest = descr[0].partition('=')
                if sep:
                    expr = descr[0].rstrip(":")
                    text = descr[1].lstrip(":- \t")
                else:
                    name, expr, text = params_descr.pop()
                    text += "\n" + ln
                params_descr.append ((name, expr, text))
            elif ln == "Parameters:":
                params_found = True
        return params_descr

    def _check_args (self, args):
        self.valid_arg_names = []
        passed_arg_names = args.keys() if args else []
        args_from_doc = self._parse_args_from_doc()
        if args_from_doc:
            self.valid_arg_names = list(zip(*args_from_doc)[0])
        invalid = set(passed_arg_names) - set(self.valid_arg_names)
        if invalid:
            raise ValueError ("Invalid handler argument: %s" % list(invalid))

    def add(self, trigger_arg_list):
        """Called when table is added.

        Can modify trigger args.
        """
        pass

    def reset(self):
        """Called before starting to process a batch.
        Should clean any pending data.
        """
        pass

    def prepare_batch(self, batch_info, dst_curs):
        """Called on first event for this table in current batch."""
        pass

    def process_event(self, ev, sql_queue_func, arg):
        """Process a event.

        Event should be added to sql_queue or executed directly.
        """
        pass

    def finish_batch(self, batch_info, dst_curs):
        """Called when batch finishes."""
        pass

    def get_copy_condition(self, src_curs, dst_curs):
        """ Use if you want to filter data """
        return ''

    def real_copy(self, src_tablename, src_curs, dst_curs, column_list):
        """do actual table copy and return tuple with number of bytes and rows
        copyed
        """
        condition = self.get_copy_condition(src_curs, dst_curs)
        return skytools.full_copy(src_tablename, src_curs, dst_curs,
                                  column_list, condition,
                                  dst_tablename = self.dest_table)

    def needs_table(self):
        """Does the handler need the table to exist on destination."""
        return True

class TableHandler(BaseHandler):
    """Default Londiste handler, inserts events into tables with plain SQL.

    Parameters:
      encoding=ENC - Validate and fix incoming data from encoding.
                     Only 'utf8' is supported at the moment.
    """
    handler_name = 'londiste'

    sql_command = {
        'I': "insert into %s %s;",
        'U': "update only %s set %s;",
        'D': "delete from only %s where %s;",
    }

    allow_sql_event = 1

    def __init__(self, table_name, args, dest_table):
        BaseHandler.__init__(self, table_name, args, dest_table)

        enc = args.get('encoding')
        if enc:
            self.enc = EncodingValidator(self.log, enc)
        else:
            self.enc = None

    def process_event(self, ev, sql_queue_func, arg):
        row = self.parse_row_data(ev)
        if len(ev.type) == 1:
            # sql event
            fqname = self.fq_dest_table
            fmt = self.sql_command[ev.type]
            sql = fmt % (fqname, row)
        else:
            # urlenc event
            pklist = ev.type[2:].split(',')
            op = ev.type[0]
            tbl = self.dest_table
            if op == 'I':
                sql = skytools.mk_insert_sql(row, tbl, pklist)
            elif op == 'U':
                sql = skytools.mk_update_sql(row, tbl, pklist)
            elif op == 'D':
                sql = skytools.mk_delete_sql(row, tbl, pklist)

        sql_queue_func(sql, arg)

    def parse_row_data(self, ev):
        """Extract row data from event, with optional encoding fixes.

        Returns either string (sql event) or dict (urlenc event).
        """

        if len(ev.type) == 1:
            if not self.allow_sql_event:
                raise Exception('SQL events not supported by this handler')
            if self.enc:
                return self.enc.validate_string(ev.data, self.table_name)
            return ev.data
        else:
            row = skytools.db_urldecode(ev.data)
            if self.enc:
                return self.enc.validate_dict(row, self.table_name)
            return row

    def real_copy(self, src_tablename, src_curs, dst_curs, column_list):
        """do actual table copy and return tuple with number of bytes and rows
        copied
        """

        if self.enc:
            def _write_hook(obj, data):
                return self.enc.validate_copy(data, column_list, src_tablename)
        else:
            _write_hook = None
        condition = self.get_copy_condition(src_curs, dst_curs)
        return skytools.full_copy(src_tablename, src_curs, dst_curs,
                                  column_list, condition,
                                  dst_tablename = self.dest_table,
                                  write_hook = _write_hook)


#------------------------------------------------------------------------------
# ENCODING VALIDATOR
#------------------------------------------------------------------------------

class EncodingValidator:
    def __init__(self, log, encoding = 'utf-8', replacement = u'\ufffd'):
        """validates the correctness of given encoding. when data contains
        illegal symbols, replaces them with <replacement> and logs the
        incident
        """

        if encoding.lower() not in ('utf8', 'utf-8'):
            raise Exception('only utf8 supported')

        self.encoding = encoding
        self.log = log
        self.columns = None
        self.error_count = 0

    def show_error(self, col, val, pfx, unew):
        if pfx:
            col = pfx + '.' + col
        self.log.info('Fixed invalid UTF8 in column <%s>', col)
        self.log.debug('<%s>: old=%r new=%r', col, val, unew)

    def validate_copy(self, data, columns, pfx=""):
        """Validate tab-separated fields"""

        ok, _unicode = skytools.safe_utf8_decode(data)
        if ok:
            return data

        # log error
        vals = data.split('\t')
        for i, v in enumerate(vals):
            ok, tmp = skytools.safe_utf8_decode(v)
            if not ok:
                self.show_error(columns[i], v, pfx, tmp)

        # return safe data
        return _unicode.encode('utf8')

    def validate_dict(self, data, pfx=""):
        """validates data in dict"""
        for k, v in data.items():
            if v:
                ok, u = skytools.safe_utf8_decode(v)
                if not ok:
                    self.show_error(k, v, pfx, u)
                    data[k] = u.encode('utf8')
        return data

    def validate_string(self, value, pfx=""):
        """validate string"""
        ok, u = skytools.safe_utf8_decode(value)
        if ok:
            return value
        _pfx = pfx and (pfx+': ') or ""
        self.log.info('%sFixed invalid UTF8 in string <%s>', _pfx, value)
        return u.encode('utf8')

#
# handler management
#

_handler_map = {
    'londiste': TableHandler,
}

_handler_list = _handler_map.keys()

def register_handler_module(modname):
    """Import and module and register handlers."""
    try:
        __import__(modname)
    except ImportError:
        print "Failed to load handler module: %s" % (modname,)
        return
    m = sys.modules[modname]
    for h in m.__londiste_handlers__:
        _handler_map[h.handler_name] = h
        _handler_list.append(h.handler_name)

def _parse_arglist(arglist):
    args = {}
    for arg in arglist or []:
        key, _, val = arg.partition('=')
        key = key.strip()
        if key in args:
            raise Exception('multiple handler arguments: %s' % key)
        args[key] = val.strip()
    return args

def create_handler_string(name, arglist):
    handler = name
    if name.find('(') >= 0:
        raise Exception('invalid handler name: %s' % name)
    if arglist:
        args = _parse_arglist(arglist)
        astr = skytools.db_urlencode(args)
        handler = '%s(%s)' % (handler, astr)
    return handler

def _parse_handler(hstr):
    """Parse result of create_handler_string()."""
    args = {}
    name = hstr
    pos = hstr.find('(')
    if pos > 0:
        name = hstr[ : pos]
        if hstr[-1] != ')':
            raise Exception('invalid handler format: %s' % hstr)
        astr = hstr[pos + 1 : -1]
        if astr:
            astr = astr.replace(',', '&')
            args = skytools.db_urldecode(astr)
    return (name, args)

def build_handler(tblname, hstr, dest_table=None):
    """Parse and initialize handler.

    hstr is result of create_handler_string()."""
    hname, args = _parse_handler(hstr)
    # when no handler specified, use londiste
    hname = hname or 'londiste'
    klass = _handler_map[hname]
    if not dest_table:
        dest_table = tblname
    return klass(tblname, args, dest_table)

def load_handler_modules(cf):
    """Load and register modules from config."""
    lst = londiste.handlers.DEFAULT_HANDLERS
    lst += cf.getlist('handler_modules', [])

    for m in lst:
        register_handler_module(m)

def show(mods):
    if not mods:
        if 0:
            names = _handler_map.keys()
            names.sort()
        else:
            names = _handler_list
        for n in names:
            kls = _handler_map[n]
            desc = kls.__doc__ or ''
            if desc:
                desc = desc.strip().split('\n', 1)[0]
            print("%s - %s" % (n, desc))
    else:
        for n in mods:
            kls = _handler_map[n]
            desc = kls.__doc__ or ''
            if desc:
                desc = desc.strip()
            print("%s - %s" % (n, desc))
