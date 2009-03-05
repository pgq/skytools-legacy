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
    "unquote_ident",
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

def quote_statement(sql, dict):
    """Quote whole statement.

    Data values are taken from dict.
    """
    xdict = {}
    for k, v in dict.items():
        xdict[k] = quote_literal(v)
    return sql % xdict

# reserved keywords
_ident_kwmap = {
"all":1, "analyse":1, "analyze":1, "and":1, "any":1, "array":1, "as":1,
"asc":1, "asymmetric":1, "both":1, "case":1, "cast":1, "check":1, "collate":1,
"column":1, "constraint":1, "create":1, "current_date":1, "current_role":1,
"current_time":1, "current_timestamp":1, "current_user":1, "default":1,
"deferrable":1, "desc":1, "distinct":1, "do":1, "else":1, "end":1, "except":1,
"false":1, "for":1, "foreign":1, "from":1, "grant":1, "group":1, "having":1,
"in":1, "initially":1, "intersect":1, "into":1, "leading":1, "limit":1,
"localtime":1, "localtimestamp":1, "new":1, "not":1, "null":1, "off":1,
"offset":1, "old":1, "on":1, "only":1, "or":1, "order":1, "placing":1,
"primary":1, "references":1, "returning":1, "select":1, "session_user":1,
"some":1, "symmetric":1, "table":1, "then":1, "to":1, "trailing":1, "true":1,
"union":1, "unique":1, "user":1, "using":1, "when":1, "where":1,
}

_ident_bad = re.compile(r"[^a-z0-9_]|^[0-9]")
def quote_ident(s):
    """Quote SQL identifier.

    If is checked against weird symbols and keywords.
    """

    if _ident_bad.search(s) or s in _ident_kwmap:
        s = '"%s"' % s.replace('"', '""')
    return s

def quote_fqident(s):
    """Quote fully qualified SQL identifier.

    The '.' is taken as namespace separator and
    all parts are quoted separately
    """
    return '.'.join(map(quote_ident, s.split('.', 1)))

#
# quoting for JSON strings
#

_jsre = re.compile(r'[\x00-\x1F\\/"]')
_jsmap = { "\b": "\\b", "\f": "\\f", "\n": "\\n", "\r": "\\r",
    "\t": "\\t", "\\": "\\\\", '"': '\\"',
    "/": "\\/",   # to avoid html attacks
}

def _json_quote_char(m):
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
    """Removes C-style escapes, also converts "\N" to None."""
    if val == r"\N":
        return None
    return unescape(val)

def unquote_ident(val):
    """Unquotes possibly quoted SQL identifier."""
    if val[0] == '"' and val[-1] == '"':
        return val[1:-1].replace('""', '"')
    return val

