
"""Various parsers for Postgres-specific data formats."""

import re

from skytools.quoting import unescape

__all__ = ["parse_pgarray", "parse_logtriga_sql", "parse_tabbed_table", "parse_statements"]

_rc_listelem = re.compile(r'( [^,"}]+ | ["] ( [^"\\]+ | [\\]. )* ["] )', re.X)

# _parse_pgarray
def parse_pgarray(array):
    """ Parse Postgres array and return list of items inside it
        Used to deserialize data recived from service layer parameters
    """
    if not array or array[0] != "{":
        raise Exception("bad array format")
    res = []
    pos = 1
    while 1:
        m = _rc_listelem.search(array, pos)
        if not m:
            break
        pos2 = m.end()
        item = array[pos:pos2]
        if len(item) > 0 and item[0] == '"':
            item = item[1:-1]
        item = unescape(item)
        res.append(item)

        pos = pos2 + 1
        if array[pos2] == "}":
            break
        elif array[pos2] != ",":
            raise Exception("bad array format")
    return res

#
# parse logtriga partial sql
#

class _logtriga_parser:
    token_re = r"""
        [ \t\r\n]*
        ( [a-z][a-z0-9_]*
        | ["] ( [^"\\]+ | \\. )* ["]
        | ['] ( [^'\\]+ | \\. | [']['] )* [']
        | [^ \t\r\n]
        )"""
    token_rc = None

    def tokenizer(self, sql):
        if not _logtriga_parser.token_rc:
            _logtriga_parser.token_rc = re.compile(self.token_re, re.X | re.I)
        rc = self.token_rc

        pos = 0
        while 1:
            m = rc.match(sql, pos)
            if not m:
                break
            pos = m.end()
            yield m.group(1)

    def unquote_data(self, fields, values):
        # unquote data and column names
        data = {}
        for k, v in zip(fields, values):
            if k[0] == '"':
                k = unescape(k[1:-1])
            if len(v) == 4 and v.lower() == "null":
                v = None
            elif v[0] == "'":
                v = unescape(v[1:-1])
            data[k] = v
        return data

    def parse_insert(self, tk, fields, values):
        # (col1, col2) values ('data', null)
        if tk.next() != "(":
            raise Exception("syntax error")
        while 1:
            fields.append(tk.next())
            t = tk.next()
            if t == ")":
                break
            elif t != ",":
                raise Exception("syntax error")
        if tk.next().lower() != "values":
            raise Exception("syntax error")
        if tk.next() != "(":
            raise Exception("syntax error")
        while 1:
            t = tk.next()
            if t == ")":
                break
            if t == ",":
                continue
            values.append(t)
        tk.next()

    def parse_update(self, tk, fields, values):
        # col1 = 'data1', col2 = null where pk1 = 'pk1' and pk2 = 'pk2'
        while 1:
            fields.append(tk.next())
            if tk.next() != "=":
                raise Exception("syntax error")
            values.append(tk.next())
            
            t = tk.next()
            if t == ",":
                continue
            elif t.lower() == "where":
                break
            else:
                raise Exception("syntax error")
        while 1:
            t = tk.next()
            fields.append(t)
            if tk.next() != "=":
                raise Exception("syntax error")
            values.append(tk.next())
            t = tk.next()
            if t.lower() != "and":
                raise Exception("syntax error")

    def parse_delete(self, tk, fields, values):
        # pk1 = 'pk1' and pk2 = 'pk2'
        while 1:
            t = tk.next()
            if t == "and":
                continue
            fields.append(t)
            if tk.next() != "=":
                raise Exception("syntax error")
            values.append(tk.next())

    def parse_sql(self, op, sql):
        tk = self.tokenizer(sql)
        fields = []
        values = []
        try:
            if op == "I":
                self.parse_insert(tk, fields, values)
            elif op == "U":
                self.parse_update(tk, fields, values)
            elif op == "D":
                self.parse_delete(tk, fields, values)
            raise Exception("syntax error")
        except StopIteration:
            # last sanity check
            if len(fields) == 0 or len(fields) != len(values):
                raise Exception("syntax error")

        return self.unquote_data(fields, values)

def parse_logtriga_sql(op, sql):
    """Parse partial SQL used by logtriga() back to data values.

    Parser has following limitations:
    - Expects standard_quoted_strings = off
    - Does not support dollar quoting.
    - Does not support complex expressions anywhere. (hashtext(col1) = hashtext(val1))
    - WHERE expression must not contain IS (NOT) NULL
    - Does not support updateing pk value.

    Returns dict of col->data pairs.
    """
    return _logtriga_parser().parse_sql(op, sql)


def parse_tabbed_table(txt):
    """Parse a tab-separated table into list of dicts.
    
    Expect first row to be column names.

    Very primitive.
    """

    txt = txt.replace("\r\n", "\n")
    fields = None
    data = []
    for ln in txt.split("\n"):
        if not ln:
            continue
        if not fields:
            fields = ln.split("\t")
            continue
        cols = ln.split("\t")
        if len(cols) != len(fields):
            continue
        row = dict(zip(fields, cols))
        data.append(row)
    return data


_sql_token_re = r"""
    ( [a-z][a-z0-9_$]*
    | ["] ( [^"\\]+ | \\. )* ["]
    | ['] ( [^'\\]+ | \\. | [']['] )* [']
    | [$] ([_a-z][_a-z0-9]*)? [$]
    | (?P<ws> \s+ | [/][*] | [-][-][^\n]* )
    | .
    )"""
_sql_token_rc = None
_copy_from_stdin_re = "copy.*from\s+stdin"
_copy_from_stdin_rc = None

def _sql_tokenizer(sql):
    global _sql_token_rc, _copy_from_stdin_rc
    if not _sql_token_rc:
        _sql_token_rc = re.compile(_sql_token_re, re.X | re.I)
        _copy_from_stdin_rc = re.compile(_copy_from_stdin_re, re.X | re.I)
    rc = _sql_token_rc

    pos = 0
    while 1:
        m = rc.match(sql, pos)
        if not m:
            break
        pos = m.end()
        tok = m.group(1)
        ws = m.start('ws') >= 0 # it tok empty?
        if tok == "/*":
            end = sql.find("*/", pos)
            if end < 0:
                raise Exception("unterminated c comment")
            pos = end + 2
            tok = sql[ m.start() : pos]
        elif len(tok) > 1 and tok[0] == "$" and tok[-1] == "$":
            end = sql.find(tok, pos)
            if end < 0:
                raise Exception("unterminated dollar string")
            pos = end + len(tok)
            tok = sql[ m.start() : pos]
        yield (ws, tok)

def parse_statements(sql):
    """Parse multi-statement string into separate statements.

    Returns list of statements.
    """

    tk = _sql_tokenizer(sql)
    tokens = []
    pcount = 0 # '(' level
    while 1:
        try:
            ws, t = tk.next()
        except StopIteration:
            break
        # skip whitespace and comments before statement
        if len(tokens) == 0 and ws:
            continue
        # keep the rest
        tokens.append(t)
        if t == "(":
            pcount += 1
        elif t == ")":
            pcount -= 1
        elif t == ";" and pcount == 0:
            sql = "".join(tokens)
            if _copy_from_stdin_rc.match(sql):
                raise Exception("copy from stdin not supported")
            yield ("".join(tokens))
            tokens = []
    if len(tokens) > 0:
        yield ("".join(tokens))
    if pcount != 0:
        raise Exception("syntax error - unbalanced parenthesis")

