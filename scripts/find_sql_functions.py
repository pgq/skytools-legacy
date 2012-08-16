#! /usr/bin/env python

"""Find and print out function signatures from .sql file.

Usage:
    find_sql_functions.py [-h] [-s] [-p PREFIX] FILE ...

Switches:
    -h         Show help
    -p PREFIX  Prefix each line with string
    -s         Check whether function is SECURITY DEFINER
"""

import sys, re, getopt

rx = r"""
^
create \s+ (?: or \s+ replace \s+ )?
function ( [^(]+ )
[(] ( [^)]* ) [)]
"""

rx_secdef = r"""security\s+definer"""


rc = re.compile(rx, re.I | re.M | re.X)
sc = re.compile(r"\s+")
rc_sec = re.compile(rx_secdef, re.I | re.X)

def grep_file(fn, cf_prefix, cf_secdef):
    sql = open(fn).read()
    pos = 0
    while 1:
        m = rc.search(sql, pos)
        if not m:
            break
        pos = m.end()

        m2 = rc.search(sql, pos)
        if m2:
            xpos = m2.end()
        else:
            xpos = len(sql)
        secdef = False
        m2 = rc_sec.search(sql, pos, xpos)
        if m2:
            secdef = True

        fname = m.group(1).strip()
        fargs = m.group(2)

        alist = fargs.split(',')
        tlist = []
        for a in alist:
            a = a.strip()
            toks = sc.split(a.lower())
            if toks[0] == "out":
                continue
            if toks[0] in ("in", "inout"):
                toks = toks[1:]
            # just take last item
            tlist.append(toks[-1])

        sig = "%s(%s)" % (fname, ", ".join(tlist))

        if cf_prefix:
            ln = "%s %s;" % (cf_prefix, sig)
        else:
            ln = "    %s(%s)," % (fname, ", ".join(tlist))

        if cf_secdef and secdef:
            ln = "%-72s -- SECDEF" % (ln)

        print ln

def main(argv):
    cf_secdef = 0
    cf_prefix = ''

    try:
        opts, args = getopt.getopt(argv, "hsp:")
    except getopt.error, d:
        print 'getopt:', d
        sys.exit(1)

    for o, a in opts:
        if o == '-h':
            print __doc__
            sys.exit(0)
        elif o == '-s':
            cf_secdef = 1
        elif o == '-p':
            cf_prefix = a
        else:
            print __doc__
            sys.exit(1)

    for fn in args:
        grep_file(fn, cf_prefix, cf_secdef)

if __name__ == '__main__':
    main(sys.argv[1:])

