#! /usr/bin/env python

import sys, os.path, re, glob
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

share_dup_files = [
   'sql/pgq/pgq.sql',
   'sql/londiste/londiste.sql',
   'sql/pgq_ext/pgq_ext.sql',
   'sql/logtriga/logtriga.sql',
]
if os.path.isfile('sql/txid/txid.sql'):
   share_dup_files.append('sql/txid/txid.sql')

# run actual setup
setup(
    name = "skytools",
    license = "BSD",
    version = ac_ver,
    maintainer = "Marko Kreen",
    maintainer_email = "marko.kreen@skype.net",
    url = "http://pgfoundry.org/projects/skytools/",
    package_dir = {'': 'python'},
    packages = ['skytools', 'londiste', 'pgq'],
    scripts = ['python/londiste.py', 'python/pgqadm.py', 'python/walmgr.py',
               'scripts/cube_dispatcher.py', 'scripts/queue_mover.py',
               'scripts/table_dispatcher.py', 'scripts/bulk_loader.py',
               'scripts/scriptmgr.py', 'scripts/queue_splitter.py',
               'scripts/skytools_upgrade.py',
              ],
    data_files = [
      ('share/doc/skytools/conf', [
        'python/conf/londiste.ini',
        'python/conf/pgqadm.ini',
        'python/conf/skylog.ini',
        'python/conf/wal-master.ini',
        'python/conf/wal-slave.ini',
        'scripts/queue_mover.ini.templ',
        'scripts/queue_splitter.ini.templ',
        'scripts/cube_dispatcher.ini.templ',
        'scripts/table_dispatcher.ini.templ',
        'scripts/bulk_loader.ini.templ',
        'scripts/scriptmgr.ini.templ',
        ]),
      ('share/skytools', share_dup_files),
      ('share/skytools/upgrade/final', glob.glob('upgrade/final/*.sql')),
      ],
    ext_modules=[Extension("skytools._cquoting", ['python/modules/cquoting.c'])],
)

