#! /usr/bin/env python

import sys, os, os.path

while 1:
    fn = sys.stdin.readline().strip()
    if not fn:
        break
    full1 = os.path.join(sys.argv[1], fn)
    full2 = os.path.splitext(full1)[0]
    if full1 == full2:
        continue
    print full1, full2
    os.rename(full1, full2)

