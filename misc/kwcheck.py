#! /usr/bin/env python

import sys
import re

import pkgloader
pkgloader.require('skytools', '3.0')
import skytools.quoting

kwmap = skytools.quoting._ident_kwmap

fn = "/opt/src/pgsql/postgresql/src/include/parser/kwlist.h"
if len(sys.argv) == 2:
    fn = sys.argv[1]

rc = re.compile(r'PG_KEYWORD[(]"(.*)" , \s* \w+ , \s* (\w+) [)]', re.X)

data = open(fn, 'r').read()
full_map = {}
cur_map = {}
print "== new =="
for kw, cat in rc.findall(data):
    full_map[kw] = 1
    if cat == 'UNRESERVED_KEYWORD':
        continue
    if cat == 'COL_NAME_KEYWORD':
        continue
    cur_map[kw] = 1
    if kw not in kwmap:
        print kw, cat
    kwmap[kw] = 1

print "== obsolete =="
kws = kwmap.keys()
kws.sort()
for k in kws:
    if k not in full_map:
        print k, '(not in full_map)'
    elif k not in cur_map:
        print k, '(not in cur_map)'

print "== full list =="
ln = ""
for k in kws:
    ln += '"%s":1, ' % k
    if len(ln) > 70:
        print ln.strip()
        ln = ""
print ln.strip()

