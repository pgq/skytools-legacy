#! /usr/bin/env python

"""Londiste launcher.

Config template::

    [londiste]
    job_name = somedb_worker

    db = dbname=somedb host=127.0.0.1

    queue_name = some_queue

    logfile = ~/log/%(job_name)s.log
    pidfile = ~/pid/%(job_name)s.pid

    # how many tables can be copied in parallel
    #parallel_copies = 1

    # sleep time between work loops
    #loop_delay = 1.0

"""

import sys, os, os.path, optparse

import pkgloader
pkgloader.require('skytools', '3.0')

import skytools

# python 2.3 will try londiste.py first...
if os.path.exists(os.path.join(sys.path[0], 'londiste.py')) \
    and not os.path.exists(os.path.join(sys.path[0], 'londiste')):
    del sys.path[0]

import londiste, pgq.cascade.admin

command_usage = pgq.cascade.admin.command_usage + """
Replication Daemon:
  worker                replay events to subscriber

Replication Administration:
  add-table TBL ...     add table to queue
  remove-table TBL ...  remove table from queue
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
  execute [FILE ...]    execute SQL files on set

Internal Commands:
  copy                  copy table logic
"""

cmd_handlers = (
    (('create-root', 'create-branch', 'create-leaf', 'members', 'tag-dead', 'tag-alive',
      'change-provider', 'rename-node', 'status', 'pause', 'resume',
      'drop-node', 'takeover'), londiste.LondisteSetup),
    (('add-table', 'remove-table', 'add-seq', 'remove-seq', 'tables', 'seqs',
      'missing', 'resync', 'check', 'fkeys', 'execute'), londiste.LondisteSetup),
    (('worker', 'replay'), londiste.Replicator),
    (('compare',), londiste.Comparator),
    (('repair',), londiste.Repairer),
    (('copy',), londiste.CopyTable),
)

class Londiste(skytools.DBScript):
    __doc__ = __doc__
    def __init__(self, args):
        skytools.DBScript.__init__(self, 'londiste', args)

        if len(self.args) < 2:
            print("need command")
            sys.exit(1)
        cmd = self.args[1]
        self.script = None
        for names, cls in cmd_handlers:
            if cmd in names:
                self.script = cls(args)
                break
        if not self.script:
            print("Unknown command '%s', use --help for help" % cmd)
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
        g.add_option("--create", action="store_true",
                help = "add: create table/seq if not exist")
        g.add_option("--create-only",
                help = "add: create table/seq if not exist (seq,pkey,full,indexes,fkeys)")
        g.add_option("--target",
                help = "switchover: target node")
        g.add_option("--merge",
                help = "create-leaf: combined queue name")
        g.add_option("--dead", action = 'append',
                help = "cascade: assume node is dead")
        g.add_option("--dead-root", action = 'store_true',
                help = "takeover: old node was root")
        g.add_option("--dead-branch", action = 'store_true',
                help = "takeover: old node was branch")
        p.add_option_group(g)
        return p

if __name__ == '__main__':
    script = Londiste(sys.argv[1:])
    script.start()

