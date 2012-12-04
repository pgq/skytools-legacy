#! /usr/bin/env python

"""Admin scripting.
"""

import sys, inspect

import skytools

__all__ = ['AdminScript']

class AdminScript(skytools.DBScript):
    """Contains common admin script tools.

    Second argument (first is .ini file) is taken as command
    name.  If class method 'cmd_' + arg exists, it is called,
    otherwise error is given.
    """
    def __init__(self, service_name, args):
        """AdminScript init."""
        skytools.DBScript.__init__(self, service_name, args)
        if self.pidfile:
            self.pidfile = self.pidfile + ".admin"

        if len(self.args) < 2:
            self.log.error("need command")
            sys.exit(1)

    def work(self):
        """Non-looping work function, calls command function."""

        self.set_single_loop(1)

        cmd = self.args[1]
        cmdargs = self.args[2:]

        # find function
        fname = "cmd_" + cmd.replace('-', '_')
        if not hasattr(self, fname):
            self.log.error('bad subcommand, see --help for usage')
            sys.exit(1)
        fn = getattr(self, fname)

        # check if correct number of arguments
        (args, varargs, varkw, defaults) = inspect.getargspec(fn)
        n_args = len(args) - 1 # drop 'self'
        if varargs is None and n_args != len(cmdargs):
            helpstr = ""
            if n_args:
                helpstr = ": " + " ".join(args[1:])
            self.log.error("command '%s' got %d args, but expects %d%s"
                    % (cmd, len(cmdargs), n_args, helpstr))
            sys.exit(1)

        # run command
        fn(*cmdargs)

    def fetch_list(self, db, sql, args, keycol = None):
        """Fetch a resultset from db, optionally turning it into value list."""
        curs = db.cursor()
        curs.execute(sql, args)
        rows = curs.fetchall()
        db.commit()
        if not keycol:
            res = rows
        else:
            res = [r[keycol] for r in rows]
        return res

    def display_table(self, db, desc, sql, args = [], fields = [],
                      fieldfmt = {}):
        """Display multirow query as a table."""

        self.log.debug("display_table: %s" % skytools.quote_statement(sql, args))
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
                rlen = row[k] and len(str(row[k])) or 0
                widths[i] = widths[i] > rlen and widths[i] or rlen
        widths = [w + 2 for w in widths]

        fmt = '%%-%ds' * (len(widths) - 1) + '%%s'
        fmt = fmt % tuple(widths[:-1])
        if desc:
            print(desc)
        print(fmt % tuple(fields))
        print(fmt % tuple([ '-' * (w - 2) for w in widths ]))
        #print(fmt % tuple(['-'*15] * len(fields)))
        for row in rows:
            vals = []
            for field in fields:
                val = row[field]
                if field in fieldfmt:
                    val = fieldfmt[field](val)
                vals.append(val)
            print(fmt % tuple(vals))
        print('\n')
        return 1

    def exec_stmt(self, db, sql, args):
        """Run regular non-query SQL on db."""
        self.log.debug("exec_stmt: %s" % skytools.quote_statement(sql, args))
        curs = db.cursor()
        curs.execute(sql, args)
        db.commit()

    def exec_query(self, db, sql, args):
        """Run regular query SQL on db."""
        self.log.debug("exec_query: %s" % skytools.quote_statement(sql, args))
        curs = db.cursor()
        curs.execute(sql, args)
        res = curs.fetchall()
        db.commit()
        return res
