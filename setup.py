#! /usr/bin/env python

from distutils.core import setup

import re
buf = open("configure.ac","r").read(256)
m = re.search("AC_INIT[(][^,]*,\s+([^)]*)[)]", buf)
ac_ver = m.group(1)

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
               'scripts/scriptmgr.py', 'scripts/queue_splitter.py'],
    data_files = [ ('share/doc/skytools/conf', [
        'python/conf/londiste.ini',
        'python/conf/pgqadm.ini',
        'python/conf/wal-master.ini',
        'python/conf/wal-slave.ini',
        'scripts/queue_mover.ini.templ',
        'scripts/queue_splitter.ini.templ',
        'scripts/cube_dispatcher.ini.templ',
        'scripts/table_dispatcher.ini.templ',
        'scripts/bulk_loader.ini.templ',
        'scripts/scriptmgr.ini.templ',
        ])]
)

