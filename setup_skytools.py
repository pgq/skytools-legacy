#! /usr/bin/env python

# this script installs Python modules, scripts and sql files

# custom switches for install:
# --script-suffix=   add suffix to scripts
# --sk3-subdir       install modules into "skytools-3.0" subdir
# --skylog           use "skylog" logging by default

import sys, os.path, re
from distutils.core import setup
from distutils.extension import Extension
from distutils.command.build import build
from distutils.command.install import install
from subprocess import Popen

INSTALL_SCRIPTS = 1
INSTALL_SQL = 1

# dont build C module on win32 as it's unlikely to have dev env
BUILD_C_MOD = 1
if sys.platform == 'win32':
    BUILD_C_MOD = 0

# load version
buf = open("configure.ac","r").read(256)
m = re.search("AC_INIT[(][^,]*,\s+([^)]*)[)]", buf)
ac_ver = m.group(1)

# scripts that we add suffix
sfx_scripts = [
    'python/londiste.py',
    'python/walmgr.py',
    'scripts/scriptmgr.py',
    'scripts/queue_splitter.py',
    'scripts/queue_mover.py',
]
# those do not need suffix (no conflict with 2.1)
nosfx_scripts = [
    'python/qadmin.py',
]

if not INSTALL_SCRIPTS:
    sfx_scripts = []
    nosfx_scripts = []

# sql files we want to access from python
sql_files = [
   'sql/pgq/pgq.sql',
   'sql/londiste/londiste.sql',
   'sql/pgq_ext/pgq_ext.sql',
   'sql/pgq_node/pgq_node.sql',
   #'sql/txid/txid.sql',
]

if not INSTALL_SQL:
    sql_files = []

def getvar(name, default):
    try:
        cf = open('config.mak').read()
        m = re.search(r'^%s *= *(.*)' % name, cf, re.M)
        if m:
            return m.group(1).strip()
    except IOError:
        pass
    return default

# dont rename scripts on win32
if sys.platform == 'win32':
    DEF_SUFFIX = '.py'
    DEF_NOSUFFIX = '.py'
else:
    DEF_SUFFIX = ''
    DEF_NOSUFFIX = ''

# load defaults from config.mak
DEF_SUFFIX = getvar('SUFFIX', DEF_SUFFIX)
DEF_SKYLOG = getvar('SKYLOG', '0') != '0'
DEF_SK3_SUBDIR = getvar('SK3_SUBDIR', '0') != '0'

# create sql files if they dont exist
for fn in sql_files:
    if not os.path.isfile(fn):
        f = open(fn, 'w')
        wd = os.path.dirname(fn)
        cmd = [sys.executable, '../../scripts/catsql.py', 'structure/install.sql']
        p = Popen(cmd, stdout=f, cwd = wd)
        p.communicate()
        if p.returncode != 0:
            raise Exception('catsql failed')

# remove .py, add suffix
def fixscript(fn, dstdir, sfx):
    fn = os.path.basename(fn)
    fn2 = fn.replace('.py', sfx)
    if fn == fn2:
        return
    dfn = os.path.join(dstdir, fn)
    dfn2 = os.path.join(dstdir, fn2)
    print("Renaming %s -> %s" % (dfn, fn2))
    if sys.platform == 'win32' and os.path.isfile(dfn2):
        os.remove(dfn2)
    os.rename(dfn, dfn2)

class sk3_build(build):
    def initialize_options(self):
        build.initialize_options(self)
        self.build_base = 'build.sk3'

# wrap generic install command
class sk3_install(install):
    user_options = install.user_options + [
            ('script-suffix=', None, 'add suffix to scripts'),
            ('sk3-subdir', None, 'install modules into "skytools-3.0" subdir'),
            ('skylog', None, 'use "skylog" logging by default'),
    ]
    boolean_options = ['sk3-subdir', 'skylog']
    sk3_subdir = DEF_SK3_SUBDIR
    script_suffix = DEF_SUFFIX
    skylog = DEF_SKYLOG

    def run(self):

        # create installer_config.py with final paths
        fn = 'python/skytools/installer_config.py'
        cf = open(fn + '.in', 'r').read()
        cf = cf.replace('@SQLDIR@', os.path.join(self.prefix, 'share/skytools3'))
        cf = cf.replace('@PACKAGE_VERSION@', ac_ver)
        cf = cf.replace('@SKYLOG@', self.skylog and '1' or '0')
        open(fn, 'w').write(cf)

        # move python modules
        if self.sk3_subdir:
            subdir = 'skytools-3.0'
            self.install_lib = os.path.join(self.install_lib, subdir)
            self.install_purelib = os.path.join(self.install_purelib, subdir)
            self.install_platlib = os.path.join(self.install_platlib, subdir)

        # generic install
        install.run(self)

        # fix scripts
        for sfn in sfx_scripts:
            fixscript(sfn, self.install_scripts, self.script_suffix)
        for sfn in nosfx_scripts:
            fixscript(sfn, self.install_scripts, DEF_NOSUFFIX)

# check if building C is allowed
c_modules = []
if BUILD_C_MOD:
    ext = Extension("skytools._cquoting", ['python/modules/cquoting.c'])
    c_modules.append(ext)

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
    ext_modules = c_modules,
    scripts = sfx_scripts + nosfx_scripts,
    cmdclass = { 'build': sk3_build, 'install': sk3_install },
)

