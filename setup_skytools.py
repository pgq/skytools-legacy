#! /usr/bin/env python

# this script does not perform full installation,
# it is meant for use from Makefile

import sys, os.path, re
from distutils.core import setup
from distutils.extension import Extension

# check if configure has run
if not os.path.isfile('config.mak'):
    print "please run ./configure && make first"
    print "Note: setup.py is supposed to be run from Makefile"
    sys.exit(1)

# load version
buf = open("configure.ac","r").read(256)
m = re.search("AC_INIT[(][^,]*,\s+([^)]*)[)]", buf)
ac_ver = m.group(1)

def getvar(name):
    cf = open('config.mak').read()
    m = re.search(r'^%s\s*=\s*(.*)' % name, cf, re.M)
    return m.group(1).strip()

sfx = getvar('SUFFIX')

share_dup_files = [
   'sql/pgq/pgq.sql',
   'sql/londiste/londiste.sql',
   'sql/pgq_ext/pgq_ext.sql',
   'sql/pgq_node/pgq_node.sql',
]
if os.path.isfile('sql/txid/txid.sql'):
   share_dup_files.append('sql/txid/txid.sql')

# run actual setup
setup(
    name = "skytools",
    license = "BSD",
    version = ac_ver,
    maintainer = "Marko Kreen",
    maintainer_email = "markokr@gmail.com",
    url = "http://pgfoundry.org/projects/skytools/",
    package_dir = {'': 'python'},
    packages = ['skytools', 'londiste', 'pgq', 'pgq.cascade'],
    data_files = [
      ('share/doc/skytools%s/conf' % sfx, [
        'python/conf/wal-master.ini',
        'python/conf/wal-slave.ini',
        ]),
      ('share/skytools' + sfx, share_dup_files)],
    ext_modules=[Extension("skytools._cquoting", ['python/modules/cquoting.c'])],
)

