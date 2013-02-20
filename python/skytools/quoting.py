# quoting.py

"""Various helpers for string quoting/unquoting."""

import re

__all__ = [
    # _pyqoting / _cquoting
    "quote_literal", "quote_copy", "quote_bytea_raw",
    "db_urlencode", "db_urldecode", "unescape",
    "unquote_literal",
    # local
    "quote_bytea_literal", "quote_bytea_copy", "quote_statement",
    "quote_ident", "quote_fqident", "quote_json", "unescape_copy",
    "unquote_ident", "unquote_fqident",
    "json_encode", "json_decode",
    "make_pgarray",
]

try:
    from skytools._cquoting import *
except ImportError:
    from skytools._pyquoting import *

# 
# SQL quoting
#

def quote_bytea_literal(s):
    """Quote bytea for regular SQL."""

    return quote_literal(quote_bytea_raw(s))

def quote_bytea_copy(s):
    """Quote bytea for COPY."""

    return quote_copy(quote_bytea_raw(s))

def quote_statement(sql, dict_or_list):
    """Quote whole statement.

    Data values are taken from dict or list or tuple.
    """
    if hasattr(dict_or_list, 'items'):
        qdict = {}
        for k, v in dict_or_list.items():
            qdict[k] = quote_literal(v)
        return sql % qdict
    else:
        qvals = [quote_literal(v) for v in dict_or_list]
        return sql % tuple(qvals)

# reserved keywords (RESERVED_KEYWORD + TYPE_FUNC_NAME_KEYWORD)
_ident_kwmap = {
"all":1, "analyse":1, "analyze":1, "and":1, "any":1, "array":1, "as":1,
"asc":1, "asymmetric":1, "authorization":1, "binary":1, "both":1, "case":1,
"cast":1, "check":1, "collate":1, "collation":1, "column":1, "concurrently":1,
"constraint":1, "create":1, "cross":1, "current_catalog":1, "current_date":1,
"current_role":1, "current_schema":1, "current_time":1, "current_timestamp":1,
"current_user":1, "default":1, "deferrable":1, "desc":1, "distinct":1,
"do":1, "else":1, "end":1, "errors":1, "except":1, "false":1, "fetch":1,
"for":1, "foreign":1, "freeze":1, "from":1, "full":1, "grant":1, "group":1,
"having":1, "ilike":1, "in":1, "initially":1, "inner":1, "intersect":1,
"into":1, "is":1, "isnull":1, "join":1, "lateral":1, "leading":1, "left":1,
"like":1, "limit":1, "localtime":1, "localtimestamp":1, "natural":1, "new":1,
"not":1, "notnull":1, "null":1, "off":1, "offset":1, "old":1, "on":1, "only":1,
"or":1, "order":1, "outer":1, "over":1, "overlaps":1, "placing":1, "primary":1,
"references":1, "returning":1, "right":1, "select":1, "session_user":1,
"similar":1, "some":1, "symmetric":1, "table":1, "then":1, "to":1, "trailing":1,
"true":1, "union":1, "unique":1, "user":1, "using":1, "variadic":1, "verbose":1,
"when":1, "where":1, "window":1, "with":1,
}

_ident_bad = re.compile(r"[^a-z0-9_]|^[0-9]")
def quote_ident(s):
    """Quote SQL identifier.

    If is checked against weird symbols and keywords.
    """

    if _ident_bad.search(s) or s in _ident_kwmap:
        s = '"%s"' % s.replace('"', '""')
    elif not s:
        return '""'
    return s

def quote_fqident(s):
    """Quote fully qualified SQL identifier.

    The '.' is taken as namespace separator and
    all parts are quoted separately

    Example:
    >>> quote_fqident('tbl')
    'public.tbl'
    >>> quote_fqident('Baz.Foo.Bar')
    '"Baz"."Foo.Bar"'
    """
    tmp = s.split('.', 1)
    if len(tmp) == 1:
        return 'public.' + quote_ident(s)
    return '.'.join(map(quote_ident, tmp))

#
# quoting for JSON strings
#

_jsre = re.compile(r'[\x00-\x1F\\/"]')
_jsmap = { "\b": "\\b", "\f": "\\f", "\n": "\\n", "\r": "\\r",
    "\t": "\\t", "\\": "\\\\", '"': '\\"',
    "/": "\\/",   # to avoid html attacks
}

def _json_quote_char(m):
    """Quote single char."""
    c = m.group(0)
    try:
        return _jsmap[c]
    except KeyError:
        return r"\u%04x" % ord(c)

def quote_json(s):
    """JSON style quoting."""
    if s is None:
        return "null"
    return '"%s"' % _jsre.sub(_json_quote_char, s)

def unescape_copy(val):
    r"""Removes C-style escapes, also converts "\N" to None.

    Example:
    >>> unescape_copy(r'baz\tfo\'o')
    "baz\tfo'o"
    >>> unescape_copy(r'\N') is None
    True
    """
    if val == r"\N":
        return None
    return unescape(val)

def unquote_ident(val):
    """Unquotes possibly quoted SQL identifier.
    
    >>> unquote_ident('Foo')
    'foo'
    >>> unquote_ident('"Wei "" rd"')
    'Wei " rd'
    """
    if len(val) > 1 and val[0] == '"' and val[-1] == '"':
        return val[1:-1].replace('""', '"')
    if val.find('"') > 0:
        raise Exception('unsupported syntax')
    return val.lower()

def unquote_fqident(val):
    """Unquotes fully-qualified possibly quoted SQL identifier.

    >>> unquote_fqident('foo')
    'foo'
    >>> unquote_fqident('"Foo"."Bar "" z"')
    'Foo.Bar " z'
    """
    tmp = val.split('.', 1)
    return '.'.join([unquote_ident(i) for i in tmp])

# accept simplejson or py2.6+ json module
# search for simplejson first as there exists
# incompat 'json' module
try:
    import simplejson as json
except ImportError:
    try:
        import json
    except:
        pass

def json_encode(val = None, **kwargs):
    """Creates JSON string from Python object.

    >>> json_encode({'a': 1})
    '{"a": 1}'
    >>> json_encode('a')
    '"a"'
    >>> json_encode(['a'])
    '["a"]'
    >>> json_encode(a=1)
    '{"a": 1}'
    """
    return json.dumps(val or kwargs)

def json_decode(s):
    """Parses JSON string into Python object.

    >>> json_decode('[1]')
    [1]
    """
    return json.loads(s)

#
# Create Postgres array
#

# any chars not in "good" set?  main bad ones: [ ,{}\"]
_pgarray_bad_rx = r"[^0-9a-z_.%&=()<>*/+-]"
_pgarray_bad_rc = None

def _quote_pgarray_elem(s):
    if s is None:
        return 'NULL'
    s = str(s)
    if _pgarray_bad_rc.search(s):
        s = s.replace('\\', '\\\\')
        return '"' + s.replace('"', r'\"') + '"'
    elif not s:
        return '""'
    return s

def make_pgarray(lst):
    r"""Formats Python list as Postgres array.
    Reverse of parse_pgarray().

    >>> make_pgarray([])
    '{}'
    >>> make_pgarray(['foo_3',1,'',None])
    '{foo_3,1,"",NULL}'
    >>> make_pgarray([None,',','\\',"'",'"',"{","}",'_'])
    '{NULL,",","\\\\","\'","\\"","{","}",_}'
    """

    global _pgarray_bad_rc
    if _pgarray_bad_rc is None:
        _pgarray_bad_rc = re.compile(_pgarray_bad_rx)

    items = [_quote_pgarray_elem(v) for v in lst]
    return '{' + ','.join(items) + '}'


if __name__ == '__main__':
    import doctest
    doctest.testmod()
