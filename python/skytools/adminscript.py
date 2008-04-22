#! /usr/bin/env python

"""Admin scripting.
"""

import sys, os, skytools

from skytools.scripting import DBScript

__all__ = ['AdminScript']

class AdminScript(DBScript):
    def __init__(self, service_name, args):
        DBScript.__init__(self, service_name, args)
        self.pidfile = self.pidfile + ".admin"

        if len(self.args) < 2:
            self.log.error("need command")
            sys.exit(1)

    def work(self):
        self.set_single_loop(1)
        cmd = self.args[1]
        fname = "cmd_" + cmd.replace('-', '_')
        if hasattr(self, fname):
            getattr(self, fname)(self.args[2:])
        else:
            self.log.error('bad subcommand, see --help for usage')
            sys.exit(1)

    def fetch_list(self, curs, sql, args, keycol = None):
        curs.execute(sql, args)
        rows = curs.dictfetchall()
        if not keycol:
            res = rows
        else:
            res = [r[keycol] for r in rows]
        return res

    def display_table(self, desc, curs, sql, args = [], fields = []):
        """Display multirow query as a table."""

        curs.execute(sql, args)
        rows = curs.fetchall()
        if len(rows) == 0:
            return 0

        if not fields:
            fields = [f[0] for f in curs.description]
        
        widths = [15] * len(fields)
        for row in rows:
            for i, k in enumerate(fields):
                rlen = row[k] and len(row) or 0
                widths[i] = widths[i] > rlen and widths[i] or rlen
        widths = [w + 2 for w in widths]

        fmt = '%%-%ds' * (len(widths) - 1) + '%%s'
        fmt = fmt % tuple(widths[:-1])
        if desc:
            print desc
        print fmt % tuple(fields)
        print fmt % tuple(['-'*15] * len(fields))
            
        for row in rows:
            print fmt % tuple([row[k] for k in fields])
        print '\n'
        return 1

    def db_display_table(self, db, desc, sql, args = [], fields = []):
        curs = db.cursor()
        res = self.display_table(desc, curs, sql, args, fields)
        db.commit()
        return res
        

    def exec_checked(self, curs, sql, args):
        curs.execute(sql, args)
        ok = True
        for row in curs.fetchall():
            level = row['ret_code'] / 100
            if level == 1:
                self.log.debug("%d %s" % (row[0], row[1]))
            elif level == 2:
                self.log.info("%d %s" % (row[0], row[1]))
            elif level == 3:
                self.log.warning("%d %s" % (row[0], row[1]))
            else:
                self.log.error("%d %s" % (row[0], row[1]))
                ok = False
        return ok

    def exec_many(self, curs, sql, baseargs, extra_list):
        ok = True
        for a in extra_list:
            tmp = self.exec_checked(curs, sql, baseargs + [a])
            ok = tmp and ok
        return ok

    def db_cmd(self, db, q, args, commit = True):
        ok = self.exec_checked(db.cursor(), q, args)
        if ok:
            if commit:
                self.log.info("COMMIT")
                db.commit()
        else:
            self.log.info("ROLLBACK")
            db.rollback()
            raise EXception("rollback")

    def db_cmd_many(self, db, sql, baseargs, extra_list, commit = True):
        curs = db.cursor()
        ok = self.exec_many(curs, sql, baseargs, extra_list)
        if ok:
            if commit:
                self.log.info("COMMIT")
                db.commit()
        else:
            self.log.info("ROLLBACK")
            db.rollback()


    def exec_sql(self, db, q, args):
        self.log.debug(q)
        curs = db.cursor()
        curs.execute(q, args)
        db.commit()

    def exec_query(self, db, q, args):
        self.log.debug(q)
        curs = db.cursor()
        curs.execute(q, args)
        res = curs.dictfetchall()
        db.commit()
        return res


