#! /usr/bin/env python

import sys, time
import skytools.psycopgwrapper
import skytools._cquoting, skytools._pyquoting
from decimal import Decimal

# create a DictCursor row
class fake_cursor:
    index = {'id': 0, 'data': 1}
    description = ['x', 'x']
dbrow = skytools.psycopgwrapper._CompatRow(fake_cursor())
dbrow[0] = '123'
dbrow[1] = 'value'

def regtest(name, func, cases):
    bad = 0
    for dat, res in cases:
        res2 = func(dat)
        if res != res2:
            print("failure: %s(%s) = %s (expected %s)" % (name, repr(dat), repr(res2), repr(res)))
            bad += 1
    if bad:
        print("%-20s: failed" % name)
    else:
        print("%-20s: OK" % name)
            

sql_literal = [
    [None, "null"],
    ["", "''"],
    ["a'b", "'a''b'"],
    [r"a\'b", r"E'a\\''b'"],
    [1, "'1'"],
    [True, "'True'"],
    [Decimal(1), "'1'"],
]
regtest("quote_literal/c", skytools._cquoting.quote_literal, sql_literal)
regtest("quote_literal/py", skytools._pyquoting.quote_literal, sql_literal)

sql_copy = [
    [None, "\\N"],
    ["", ""],
    ["a'\tb", "a'\\tb"],
    [r"a\'b", r"a\\'b"],
    [1, "1"],
    [True, "True"],
    [u"qwe", "qwe"],
    [Decimal(1), "1"],
]
regtest("quote_copy/c", skytools._cquoting.quote_copy, sql_copy)
regtest("quote_copy/py", skytools._pyquoting.quote_copy, sql_copy)

sql_bytea_raw = [
    [None, None],
    ["", ""],
    ["a'\tb", "a'\\011b"],
    [r"a\'b", r"a\\'b"],
    ["\t\344", r"\011\344"],
]
regtest("quote_bytea_raw/c", skytools._cquoting.quote_bytea_raw, sql_bytea_raw)
regtest("quote_bytea_raw/py", skytools._pyquoting.quote_bytea_raw, sql_bytea_raw)

sql_ident = [
    ["", ""],
    ["a'\t\\\"b", '"a\'\t\\""b"'],
    ['abc_19', 'abc_19'],
    ['from', '"from"'],
    ['0foo', '"0foo"'],
    ['mixCase', '"mixCase"'],
]
regtest("quote_ident", skytools.quote_ident, sql_ident)

t_urlenc = [
    [{}, ""],
    [{'a': 1}, "a=1"],
    [{'a': None}, "a"],
    [{'qwe': 1, u'zz': u"qwe"}, "qwe=1&zz=qwe"],
    [{'a': '\000%&'}, "a=%00%25%26"],
    [dbrow, 'data=value&id=123'],
    [{'a': Decimal("1")}, "a=1"],
]
regtest("db_urlencode/c", skytools._cquoting.db_urlencode, t_urlenc)
regtest("db_urlencode/py", skytools._pyquoting.db_urlencode, t_urlenc)

t_urldec = [
    ["", {}],
    ["a=b&c", {'a': 'b', 'c': None}],
    ["&&b=f&&", {'b': 'f'}],
    [u"abc=qwe", {'abc': 'qwe'}],
    ["b=", {'b': ''}],
    ["b=%00%45", {'b': '\x00E'}],
]
regtest("db_urldecode/c", skytools._cquoting.db_urldecode, t_urldec)
regtest("db_urldecode/py", skytools._pyquoting.db_urldecode, t_urldec)

t_unesc = [
    ["", ""],
    ["\\N", "N"],
    ["abc", "abc"],
    [u"abc", "abc"],
    [r"\0\000\001\01\1", "\0\000\001\001\001"],
    [r"a\001b\tc\r\n", "a\001b\tc\r\n"],
]
regtest("unescape/c", skytools._cquoting.unescape, t_unesc)
regtest("unescape/py", skytools._pyquoting.unescape, t_unesc)

