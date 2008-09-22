#! /usr/bin/env python

import sys, os, re, skytools

ver_rx = r"(\d+)([.](\d+)([.](\d+))?)?"
ver_rc = re.compile(ver_rx)

def detect_londiste215(curs):
    return skytools.exists_table(curs, 'londiste.subscriber_pending_fkeys')

version_list = [
 ['pgq', '2.1.5', 'v2.1.5_pgq_core.sql', None],
 # those vers did not have version func
 ['pgq_ext', '2.1.5', 'v2.1.5_pgq_ext.sql', None], # ok to reapply
 ['londiste', '2.1.5', 'v2.1.5_londiste.sql', detect_londiste215], # not ok to reapply

 ['pgq_ext', '2.1.6', 'v2.1.6_pgq_ext.sql', None],
 ['londiste', '2.1.6', 'v2.1.6_londiste.sql', None],

 ['pgq', '2.1.7', 'v2.1.7_pgq_core.sql', None],
 ['londiste', '2.1.7', 'v2.1.7_londiste.sql', None],

 ['pgq', '2.1.8', 'v2.1.8_pgq_core.sql', None],
]

def parse_ver(ver):
    m = ver_rc.match(ver)
    if not ver: return 0
    v0 = int(m.group(1) or "0")
    v1 = int(m.group(3) or "0")
    v2 = int(m.group(5) or "0")
    return ((v0 * 100) + v1) * 100 + v2

def check_version(curs, schema, new_ver_str, recheck_func=None):
    funcname = "%s.version" % schema
    if not skytools.exists_function(curs, funcname, 0):
        if recheck_func is not None:
            return recheck_func(curs)
        else:
            return 0
    q = "select %s()" % funcname
    curs.execute(q)
    old_ver_str = curs.fetchone()[0]
    new_ver = parse_ver(new_ver_str)
    old_ver = parse_ver(old_ver_str)
    return old_ver >= new_ver
    

class DbUpgrade(skytools.DBScript):
    def upgrade(self, db):
        curs = db.cursor()
        for schema, ver, sql, recheck_fn in version_list:
            if not skytools.exists_schema(curs, schema):
                continue

            if check_version(curs, schema, ver, recheck_fn):
                continue

            fn = "upgrade/final/%s" % sql
            skytools.installer_apply_file(db, fn, self.log)

    def work(self):
        self.set_single_loop(1)
        
        # loop over hosts
        for cstr in self.args:
            db = self.get_database('db', connstr = cstr, autocommit = 1)
            self.upgrade(db)
            self.close_database('db')

    def load_config(self):
         return skytools.Config(self.service_name, None,
                 user_defs = {'use_skylog': '0', 'job_name': 'db_upgrade'})

if __name__ == '__main__':
    script = DbUpgrade('db_upgrade', sys.argv[1:])
    script.start()

