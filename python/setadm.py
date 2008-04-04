#! /usr/bin/env python

import sys, pgq.setadmin

if __name__ == '__main__':
    script = pgq.setadmin.SetAdmin('set_admin', sys.argv[1:])
    script.start()

