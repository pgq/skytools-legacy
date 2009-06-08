#! /usr/bin/env python

"""SetAdmin launcher.
"""

import sys

import pkgloader
pkgloader.require('skytools', '3.0')

import pgq.cascade.admin

if __name__ == '__main__':
    script = pgq.cascade.admin.CascadeAdmin('cascade_admin', 'node_db', sys.argv[1:], worker_setup = False)
    script.start()

