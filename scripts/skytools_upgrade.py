#! /usr/bin/env python

import sys, os, re, skytools

ver_rx = r"(\d+)([.](\d+)([.](\d+))?)?"
ver_rc = re.compile(ver_rx)

version_list = [
 ['pgq', '2.1.5', 'v2.1.5_pgq_core.sql'],
 ['pgq_ext', '2.1.5', 'v2.1.5_pgq_ext.sql'],
 ['londiste', '2.1.5', 'v2.1.5_londiste.sql'],

 ['pgq_ext', '2.1.6', 'v2.1.6_pgq_ext.sql'],
 ['londiste', '2.1.6', 'v2.1.6_londiste.sql'],
]

def parse_ver(ver):
    m = ver_rc.match(ver)
    if not ver: return 0
    v0 = int(m.group(1) or "0")
    v1 = int(m.group(3) or "0")
    v2 = int(m.group(5) or "0")
    return ((v0 * 100) + v1) * 100 + v2

def check_version(curs, schema, new_ver_str):
    funcname = "%s.version" % schema
    if not skytools.exists_function(curs, funcname, 0):
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
        for schema, ver, sql in version_list:
            if not skytools.exists_schema(curs, schema):
                continue

            if check_version(curs, schema, ver):
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

