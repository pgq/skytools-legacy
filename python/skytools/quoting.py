# quoting.py

"""Various helpers for string quoting/unquoting."""

import psycopg, urllib, re

# 
# SQL quoting
#

def quote_literal(s):
    """Quote a literal value for SQL.
    
    Surronds it with single-quotes.
    """

    if s == None:
        return "null"
    s = psycopg.QuotedString(str(s))
    return str(s)

def quote_copy(s):
    """Quoting for copy command."""

    if s == None:
        return "\\N"
    s = str(s)
    s = s.replace("\\", "\\\\")
    s = s.replace("\t", "\\t")
    s = s.replace("\n", "\\n")
    s = s.replace("\r", "\\r")
    return s

def quote_bytea_raw(s):
    """Quoting for bytea parser."""

    if s == None:
        return None
    return s.replace("\\", "\\\\").replace("\0", "\\000")

def quote_bytea_literal(s):
    """Quote bytea for regular SQL."""

    return quote_literal(quote_bytea_raw(s))

def quote_bytea_copy(s):
    """Quote bytea for COPY."""

    return quote_copy(quote_bytea_raw(s))

def quote_statement(sql, dict):
    """Quote whose statement.

    Data values are taken from dict.
    """
    xdict = {}
    for k, v in dict.items():
        xdict[k] = quote_literal(v)
    return sql % xdict

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

#
# Database specific urlencode and urldecode.
#

def db_urlencode(dict):
    """Database specific urlencode.

    Encode None as key without '='.  That means that in "foo&bar=",
    foo is NULL and bar is empty string.
    """

    elem_list = []
    for k, v in dict.items():
        if v is None:
            elem = urllib.quote_plus(str(k))
        else:
            elem = urllib.quote_plus(str(k)) + '=' + urllib.quote_plus(str(v))
        elem_list.append(elem)
    return '&'.join(elem_list)

def db_urldecode(qs):
    """Database specific urldecode.

    Decode key without '=' as None.
    This also does not support one key several times.
    """

    res = {}
    for elem in qs.split('&'):
        if not elem:
            continue
        pair = elem.split('=', 1)
        name = urllib.unquote_plus(pair[0])
        if len(pair) == 1:
            res[name] = None
        else:
            res[name] = urllib.unquote_plus(pair[1])
    return res

#
# Remove C-like backslash escapes
#

_esc_re = r"\\([0-7][0-7][0-7]|.)"
_esc_rc = re.compile(_esc_re)
_esc_map = {
    't': '\t',
    'n': '\n',
    'r': '\r',
    'a': '\a',
    'b': '\b',
    "'": "'",
    '"': '"',
    '\\': '\\',
}

def _sub_unescape(m):
    v = m.group(1)
    if len(v) == 1:
        return _esc_map[v]
    else:
        return chr(int(v, 8))

def unescape(val):
    """Removes C-style escapes from string."""
    return _esc_rc.sub(_sub_unescape, val)

def unescape_copy(val):
    """Removes C-style escapes, also converts "\N" to None."""
    if val == r"\N":
        return None
    return unescape(val)

