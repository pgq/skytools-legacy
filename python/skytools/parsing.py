
"""Various parsers for Postgres-specific data formats."""

import re

from skytools.quoting import unescape, unquote_literal, unquote_ident

__all__ = ["parse_pgarray", "parse_logtriga_sql", "parse_tabbed_table", "parse_statements"]

_rc_listelem = re.compile(r'( [^,"}]+ | ["] ( [^"\\]+ | [\\]. )* ["] )', re.X)

# _parse_pgarray
def parse_pgarray(array):
    """ Parse Postgres array and return list of items inside it
        Used to deserialize data recived from service layer parameters
    """
    if not array or array[0] != "{":
        raise Exception("bad array format: must start with {")
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
            raise Exception("bad array format: expected ,} got " + array[pos2])
    return res

#
# parse logtriga partial sql
#

class _logtriga_parser:
    def tokenizer(self, sql):
        for typ, tok in sql_tokenizer(sql, ignore_whitespace = True):
            yield tok

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
            raise Exception("syntax error, expected VALUES")
        if tk.next() != "(":
            raise Exception("syntax error, expected (")
        while 1:
            values.append(tk.next())
            t = tk.next()
            if t == ")":
                break
            if t == ",":
                continue
            raise Exception("expected , or ) got "+t)
        t = tk.next()
        raise Exception("expected EOF, got " + repr(t))

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
                raise Exception("syntax error, expected WHERE or , got "+repr(t))
        while 1:
            fields.append(tk.next())
            if tk.next() != "=":
                raise Exception("syntax error")
            values.append(tk.next())
            t = tk.next()
            if t.lower() != "and":
                raise Exception("syntax error, expected AND got "+repr(t))

    def parse_delete(self, tk, fields, values):
        # pk1 = 'pk1' and pk2 = 'pk2'
        while 1:
            fields.append(tk.next())
            if tk.next() != "=":
                raise Exception("syntax error")
            values.append(tk.next())
            t = tk.next()
            if t.lower() != "and":
                raise Exception("syntax error, expected AND, got "+repr(t))

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
                raise Exception("syntax error, fields do not match values")
        fields = [unquote_ident(f) for f in fields]
        values = [unquote_literal(f) for f in values]
        return dict(zip(fields, values))

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


_extstr = r""" ['] (?: [^'\\]+ | \\. | [']['] )* ['] """
_stdstr = r""" ['] (?: [^']+ | [']['] )* ['] """
_base_sql = r"""
      (?P<ident>  [a-z][a-z0-9_$]* | ["] (?: [^"]+ | ["]["] )* ["] )
    | (?P<dolq>   (?P<dname> [$] (?: [_a-z][_a-z0-9]*)? [$] )
                  .*?
                  (?P=dname) )
    | (?P<num>    [0-9][0-9.e]* )
    | (?P<numarg> [$] [0-9]+ )
    | (?P<pyold>  [%][(] [a-z0-9_]+ [)][s] | [%][%] )
    | (?P<pynew>  [{] [^}]+ [}] | [{][{] | [}] [}] )
    | (?P<ws>     (?: \s+ | [/][*] .*? [*][/] | [-][-][^\n]* )+ )
    | (?P<sym>    . )"""
_std_sql = r"""(?: (?P<str> [E] %s | %s ) | %s )""" % (_extstr, _stdstr, _base_sql)
_ext_sql = r"""(?: (?P<str> [E]? %s ) | %s )""" % (_extstr, _base_sql)
_std_sql_rc = _ext_sql_rc = None

def sql_tokenizer(sql, standard_quoting = False, ignore_whitespace = False):
    """Parser SQL to tokens.

    Iterator, returns (toktype, tokstr) tuples.
    """
    global _std_sql_rc, _ext_sql_rc
    if not _std_sql_rc:
        _std_sql_rc = re.compile(_std_sql, re.X | re.I | re.S)
        _ext_sql_rc = re.compile(_ext_sql, re.X | re.I | re.S)

    if standard_quoting:
        rc = _std_sql_rc
    else:
        rc = _ext_sql_rc

    pos = 0
    while 1:
        m = rc.match(sql, pos)
        if not m:
            break
        pos = m.end()
        typ = m.lastgroup
        if not ignore_whitespace or typ != "ws":
            yield (m.lastgroup, m.group())

_copy_from_stdin_re = "copy.*from\s+stdin"
_copy_from_stdin_rc = None
def parse_statements(sql, standard_quoting = False):
    """Parse multi-statement string into separate statements.

    Returns list of statements.
    """

    global _copy_from_stdin_rc
    if not _copy_from_stdin_rc:
        _copy_from_stdin_rc = re.compile(_copy_from_stdin_re, re.X | re.I)
    tokens = []
    pcount = 0 # '(' level
    for typ, t in sql_tokenizer(sql, standard_quoting = standard_quoting):
        # skip whitespace and comments before statement
        if len(tokens) == 0 and typ == "ws":
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

