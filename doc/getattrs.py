#! /usr/bin/env python

import sys

buf = open(sys.argv[1], "r").read().lower()

if buf.find("pgq consumer") >= 0:
    print "-a pgq"

