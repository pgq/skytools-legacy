#! /usr/bin/env python

# this script installs only Python modules,
# scripts and sql files are installed from makefile

import sys, os.path, re
from distutils.core import setup
from distutils.extension import Extension
from distutils.command.install import install
from subprocess import Popen

# load version
buf = open("configure.ac","r").read(256)
m = re.search("AC_INIT[(][^,]*,\s+([^)]*)[)]", buf)
ac_ver = m.group(1)

sfx_scripts = [
    'python/londiste.py',
    'python/walmgr.py',
    'scripts/scriptmgr.py',
    'scripts/queue_splitter.py',
    'scripts/queue_mover.py',
]
nosfx_scripts = [
    'python/qadmin.py',
]

sql_files = [
   'sql/pgq/pgq.sql',
   'sql/londiste/londiste.sql',
   'sql/pgq_ext/pgq_ext.sql',
   'sql/pgq_node/pgq_node.sql',
   #'sql/txid/txid.sql',
]
for fn in sql_files:
    if not os.path.isfile(fn):
        f = open(fn, 'w')
        wd = os.path.dirname(fn)
        cmd = [sys.executable, '../../scripts/catsql.py', 'structure/install.sql']
        p = Popen(cmd, stdout=f, cwd = wd)
        p.communicate()
        if p.returncode != 0:
            raise Exception('catsql failed')

def fixscript(fn, dstdir, sfx):
    fn = os.path.basename(fn)
    fn2 = fn.replace('.py', sfx)
    print("Renaming %s -> %s" % (fn, fn2))
    dfn = os.path.join(dstdir, fn)
    dfn2 = os.path.join(dstdir, fn2)
    os.rename(dfn, dfn2)

class sk3_install(install):
    user_options = install.user_options + [
            ('script-suffix=', None, 'add suffix to scripts'),
            ('sk3-subdir', None, 'install modules into "skytools-3.0" subdir')
    ]
    boolean_options = ['sk3-subdir']
    sk3_subdir = ''
    script_suffix = ''

    def run(self):
        fn = 'python/skytools/installer_config.py'
        cf = open(fn + '.in', 'r').read()
        cf = cf.replace('@SQLDIR@', self.prefix + 'share/skytools3')
        cf = cf.replace('@PACKAGE_VERSION@', ac_ver)
        cf = cf.replace('@SKYLOG@', '1')
        open(fn, 'w').write(cf)

        if self.sk3_subdir:
            subdir = 'skytools-3.0'
            self.install_lib = os.path.join(self.install_lib, subdir)
            self.install_purelib = os.path.join(self.install_purelib, subdir)
            self.install_platlib = os.path.join(self.install_platlib, subdir)

        install.run(self)

        for sfn in sfx_scripts:
            fixscript(sfn, self.install_scripts, self.script_suffix)
        for sfn in nosfx_scripts:
            fixscript(sfn, self.install_scripts, '')

# run actual setup
setup(
    name = "skytools",
    license = "BSD",
    version = ac_ver,
    maintainer = "Marko Kreen",
    maintainer_email = "markokr@gmail.com",
    url = "http://pgfoundry.org/projects/skytools/",
    package_dir = {'': 'python'},
    packages = ['skytools', 'londiste', 'londiste.handlers', 'pgq', 'pgq.cascade'],
    data_files = [
      ('share/doc/skytools3/conf', [
        'python/conf/wal-master.ini',
        'python/conf/wal-slave.ini',
        ]),
      ('share/skytools3', sql_files)],
    ext_modules=[Extension("skytools._cquoting", ['python/modules/cquoting.c'])],
    scripts = sfx_scripts + nosfx_scripts,
    cmdclass = { 'install': sk3_install },
)

