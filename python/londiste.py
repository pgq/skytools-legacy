#! /usr/bin/env python

"""Londiste launcher.
"""

import sys, os, optparse, skytools, pgq, pgq.setadmin

# python 2.3 will try londiste.py first...
import sys, os.path
if os.path.exists(os.path.join(sys.path[0], 'londiste.py')) \
    and not os.path.exists(os.path.join(sys.path[0], 'londiste')):
    del sys.path[0]

import londiste

command_usage = """
%prog [options] INI CMD [subcmd args]

Node Initialization:
  init-root   NODE_NAME NODE_CONSTR
  init-branch NODE_NAME NODE_CONSTR --provider=<constr>
  init-leaf   NODE_NAME NODE_CONSTR --provider=<constr>
    Initializes node.  Given connstr is kept as global connstring
    for that node.  Those commands ignore node_db in .ini.
    The --provider connstr is used only for initial set info
    fetching, later actual provider's connect string is used.

Node Administration:
  members               Show members in set
  tag-dead NODE ..      Tag node as dead
  tag-alive NODE ..     Tag node as alive

  redirect              Switch provider
  make-root             Promote to root

Replication Daemon:
  worker                replay events to subscriber

Replication Administration:
  add TBL ...           add table to queue
  remove TBL ...        remove table from queue
  add-seq SEQ ...       add sequence to provider
  remove-seq SEQ ...    remove sequence from provider
  tables                show all tables on provider
  seqs                  show all sequences on provider
  missing               list tables subscriber has not yet attached to
  resync TBL ...        do full copy again

Replication Extra:
  check                 compare table structure on both sides
  fkeys                 print out fkey drop/create commands
  compare [TBL ...]     compare table contents on both sides
  repair [TBL ...]      repair data on subscriber

Internal Commands:
  copy                  copy table logic
"""

class NodeSetup(pgq.setadmin.SetAdmin):
    initial_db_name = 'node_db'
    extra_objs = [ skytools.DBSchema("londiste", sql_file="londiste.sql") ]
    def __init__(self, args):
        pgq.setadmin.SetAdmin.__init__(self, 'londiste', args)
    def extra_init(self, node_type, node_db, provider_db):
        if not provider_db:
            return
        pcurs = provider_db.cursor()
        ncurs = node_db.cursor()
        q = "select table_name from londiste.set_get_table_list(%s)"
        pcurs.execute(q, [self.set_name])
        for row in pcurs.fetchall():
            tbl = row['table_name']
            q = "select * from londiste.set_add_table(%s, %s)"
            ncurs.execute(q, [self.set_name, tbl])
        node_db.commit()
        provider_db.commit()


cmd_handlers = (
    (('init-root', 'init-branch', 'init-leaf', 'members', 'tag-dead', 'tag-alive',
      'redirect', 'promote-root'), NodeSetup),
    (('worker', 'replay'), londiste.Replicator),
    (('add', 'remove', 'add-seq', 'remove-seq', 'tables', 'seqs',
      'missing', 'resync', 'check', 'fkeys'), londiste.LondisteSetup),
    (('compare',), londiste.Comparator),
    (('repair',), londiste.Repairer),
    (('copy',), londiste.CopyTable),
)

class Londiste(skytools.DBScript):
    def __init__(self, args):
        skytools.DBScript.__init__(self, 'londiste', args)

        if len(self.args) < 2:
            print "need command"
            sys.exit(1)
        cmd = self.args[1]
        self.script = None
        for names, cls in cmd_handlers:
            if cmd in names:
                self.script = cls(args)
                break
        if not self.script:
            print "Unknown command '%s', use --help for help" % cmd
            sys.exit(1)

    def start(self):
        self.script.start()

    def init_optparse(self, parser=None):
        p = skytools.DBScript.init_optparse(self, parser)
        p.set_usage(command_usage.strip())

        g = optparse.OptionGroup(p, "expert options")
        g.add_option("--all", action="store_true",
                help = "add: include add possible tables")
        g.add_option("--force", action="store_true",
                help = "add: ignore table differences, repair: ignore lag")
        g.add_option("--expect-sync", action="store_true", dest="expect_sync",
                help = "add: no copy needed", default=False)
        g.add_option("--skip-truncate", action="store_true", dest="skip_truncate",
                help = "add: keep old data", default=False)
        g.add_option("--provider",
                help = "init: upstream node temp connect string")
        g.add_option("--create", action = 'callback', callback = self.opt_create_cb, type='string',
                help = "add: create table/seq if not exist")
        p.add_option_group(g)

        return p
    def opt_create_cb(self, option, opt_str, value, parser):
        print opt_str, '=', value

if __name__ == '__main__':
    script = Londiste(sys.argv[1:])
    script.start()

