#! /usr/bin/env python

"""Upgrade script for versioned schemas."""

usage = """
    %prog [--user=U] [--host=H] [--port=P] --all
    %prog [--user=U] [--host=H] [--port=P] DB1 [ DB2 ... ]\
"""

import sys, os, re, optparse

import pkgloader
pkgloader.require('skytools', '3.0')
import skytools
from skytools.natsort import natsort_key


# schemas, where .upgrade.sql is enough
AUTO_UPGRADE = ('pgq', 'pgq_node', 'pgq_coop', 'londiste', 'pgq_ext')

# fetch list of databases
DB_LIST = "select datname from pg_database "\
          " where not datistemplate and datallowconn "\
          " order by 1"

# dont support upgrade from 2.x (yet?)
version_list = [
    # schema, ver, filename, recheck_func
    ['pgq', '3.0', None, None],
    ['londiste', '3.0', None, None],
    ['pgq_ext', '2.1', None, None],
]


def is_version_ge(a, b):
    """Return True if a is greater or equal than b."""
    va = natsort_key(a)
    vb = natsort_key(b)
    return va >= vb


def check_version(curs, schema, new_ver_str, recheck_func=None):
    funcname = "%s.version" % schema
    if not skytools.exists_function(curs, funcname, 0):
        if recheck_func is not None:
            return recheck_func(curs), 'NULL'
        else:
            return 0, 'NULL'
    q = "select %s()" % funcname
    curs.execute(q)
    old_ver_str = curs.fetchone()[0]
    ok = is_version_ge(old_ver_str, new_ver_str)
    return ok, old_ver_str


class DbUpgrade(skytools.DBScript):
    """Upgrade all Skytools schemas in Postgres cluster."""

    def upgrade(self, dbname, db):
        """Upgrade all schemas in single db."""

        curs = db.cursor()
        ignore = {}
        for schema, ver, fn, recheck_func in version_list:
            # skip schema?
            if schema in ignore:
                continue
            if not skytools.exists_schema(curs, schema):
                ignore[schema] = 1
                continue

            # new enough?
            ok, oldver = check_version(curs, schema, ver, recheck_func)
            if ok:
                continue

            # too old schema, no way to upgrade
            if fn is None:
                self.log.info('%s: Cannot upgrade %s, too old version', dbname, schema)
                ignore[schema] = 1
                continue

            if self.options.not_really:
                self.log.info ("%s: Would upgrade '%s' version %s to %s", dbname, schema, oldver, ver)
                continue

            curs = db.cursor()
            curs.execute('begin')
            self.log.info("%s: Upgrading '%s' version %s to %s", dbname, schema, oldver, ver)
            skytools.installer_apply_file(db, fn, self.log)
            curs.execute('commit')

    def work(self):
        """Loop over databases."""

        self.set_single_loop(1)

        self.load_cur_versions()

        # loop over all dbs
        dblst = self.args
        if self.options.all:
            db = self.connect_db('postgres')
            curs = db.cursor()
            curs.execute(DB_LIST)
            dblst = []
            for row in curs.fetchall():
                dblst.append(row[0])
            self.close_database('db')
        elif not dblst:
            raise skytools.UsageError('Give --all or list of database names on command line')

        # loop over connstrs
        for dbname in dblst:
            if self.last_sigint:
                break
            self.log.info("%s: connecting", dbname)
            db = self.connect_db(dbname)
            self.upgrade(dbname, db)
            self.close_database('db')

    def load_cur_versions(self):
        """Load current version numbers from .upgrade.sql files."""

        vrc = re.compile(r"^ \s+ return \s+ '([0-9.]+)';", re.X | re.I | re.M)
        for s in AUTO_UPGRADE:
            fn = '%s.upgrade.sql' % s
            fqfn = skytools.installer_find_file(fn)
            try:
                f = open(fqfn, 'r')
            except IOError, d:
                raise skytools.UsageError('%s: cannot find upgrade file: %s [%s]' % (s, fqfn, str(d)))

            sql = f.read()
            f.close()
            m = vrc.search(sql)
            if not m:
                raise skytools.UsageError('%s: failed to detect version' % fqfn)

            ver = m.group(1)
            cur = [s, ver, fn, None]
            self.log.info("Loaded %s %s from %s", s, ver, fqfn)
            version_list.append(cur)

    def connect_db(self, dbname):
        """Create connect string, then connect."""

        elems = ["dbname='%s'" % dbname]
        if self.options.host:
            elems.append("host='%s'" % self.options.host)
        if self.options.port:
            elems.append("port='%s'" % self.options.port)
        if self.options.user:
            elems.append("user='%s'" % self.options.user)
        cstr = ' '.join(elems)
        return self.get_database('db', connstr = cstr, autocommit = 1)

    def init_optparse(self, parser=None):
        """Setup command-line flags."""
        p = skytools.DBScript.init_optparse(self, parser)
        p.set_usage(usage)
        g = optparse.OptionGroup(p, "options for skytools_upgrade")
        g.add_option("--all", action="store_true", help = 'upgrade all databases')
        g.add_option("--not-really", action = "store_true", dest = "not_really",
                     default = False, help = "don't actually do anything")
        g.add_option("--user", help = 'username to use')
        g.add_option("--host", help = 'hostname to use')
        g.add_option("--port", help = 'port to use')
        p.add_option_group(g)
        return p

    def load_config(self):
        """Disable config file."""
        return skytools.Config(self.service_name, None,
                user_defs = {'use_skylog': '0', 'job_name': 'db_upgrade'})

if __name__ == '__main__':
    script = DbUpgrade('skytools_upgrade', sys.argv[1:])
    script.start()
