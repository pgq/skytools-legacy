#! /usr/bin/env python

import sys, re

def inc(m):
    v = int(m.group(1))
    return str(v + 1) + "'"

func = open(sys.argv[1], 'r').read()

rc = re.compile(r"([0-9]+)'")
nfunc = rc.sub(inc, func)

rc2 = re.compile(r"'([0-9.]+)'")
nver = rc2.search(nfunc).group(1)

#print func
#print nfunc
print nver

open(sys.argv[1], 'w').write(nfunc)

