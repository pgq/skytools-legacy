#! /usr/bin/env python

"""Admin scripting.
"""

import sys, os

from skytools.scripting import DBScript
from skytools.quoting import quote_statement

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

    def fetch_list(self, db, sql, args, keycol = None):
        curs = db.cursor()
        curs.execute(sql, args)
        rows = curs.fetchall()
        db.commit()
        if not keycol:
            res = rows
        else:
            res = [r[keycol] for r in rows]
        return res

    def display_table(self, db, desc, sql, args = [], fields = []):
        """Display multirow query as a table."""

        self.log.debug("display_table: %s" % quote_statement(sql, args))
        curs = db.cursor()
        curs.execute(sql, args)
        rows = curs.fetchall()
        db.commit()
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


    def _exec_cmd(self, curs, sql, args):
        self.log.debug("exec_cmd: %s" % quote_statement(sql, args))
        curs.execute(sql, args)
        ok = True
        rows = curs.fetchall()
        for row in rows:
            try:
                code = row['ret_code']
                msg = row['ret_note']
            except KeyError:
                self.log.error("Query does not conform to exec_cmd API:")
                self.log.error("SQL: %s" % quote_statement(sql, args))
                self.log.error("Row: %s" % repr(row.copy()))
                sys.exit(1)
            level = code / 100
            if level == 1:
                self.log.debug("%d %s" % (code, msg))
            elif level == 2:
                self.log.info("%d %s" % (code, msg))
            elif level == 3:
                self.log.warning("%d %s" % (code, msg))
            else:
                self.log.error("%d %s" % (code, msg))
                self.log.error("Query was: %s" % skytools.quote_statement(sql, args))
                ok = False
        return (ok, rows)

    def _exec_cmd_many(self, curs, sql, baseargs, extra_list):
        ok = True
        rows = []
        for a in extra_list:
            (tmp_ok, tmp_rows) = self._exec_cmd(curs, sql, baseargs + [a])
            ok = tmp_ok and ok
            rows += tmp_rows
        return (ok, rows)

    def exec_cmd(self, db, q, args, commit = True):
        (ok, rows) = self._exec_cmd(db.cursor(), q, args)
        if ok:
            if commit:
                self.log.info("COMMIT")
                db.commit()
            return rows
        else:
            self.log.info("ROLLBACK")
            db.rollback()
            raise EXception("rollback")

    def exec_cmd_many(self, db, sql, baseargs, extra_list, commit = True):
        curs = db.cursor()
        (ok, rows) = self._exec_cmd_many(curs, sql, baseargs, extra_list)
        if ok:
            if commit:
                self.log.info("COMMIT")
                db.commit()
            return rows
        else:
            self.log.info("ROLLBACK")
            db.rollback()
            raise EXception("rollback")


    def exec_stmt(self, db, sql, args):
        self.log.debug("exec_stmt: %s" % quote_statement(sql, args))
        curs = db.cursor()
        curs.execute(sql, args)
        db.commit()

    def exec_query(self, db, sql, args):
        self.log.debug("exec_query: %s" % quote_statement(sql, args))
        curs = db.cursor()
        curs.execute(sql, args)
        res = curs.fetchall()
        db.commit()
        return res


