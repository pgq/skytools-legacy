#! /usr/bin/env python

"""Londiste launcher.
"""

import sys, os, optparse, skytools

# python 2.3 will try londiste.py first...
import sys, os.path
if os.path.exists(os.path.join(sys.path[0], 'londiste.py')) \
    and not os.path.exists(os.path.join(sys.path[0], 'londiste')):
    del sys.path[0]

from londiste import *

command_usage = """
%prog [options] INI CMD [subcmd args]

commands:
  provider install           installs modules, creates queue
  provider add TBL ...       add table to queue
  provider remove TBL ...    remove table from queue
  provider tables            show all tables linked to queue
  provider seqs              show all sequences on provider

  subscriber install         installs schema
  subscriber add TBL ...     add table to subscriber
  subscriber remove TBL ...  remove table from subscriber
  subscriber tables          list tables subscriber has attached to
  subscriber seqs            list sequences subscriber is interested
  subscriber missing         list tables subscriber has not yet attached to
  subscriber link QUE        create mirror queue
  subscriber unlink          dont mirror queue

  replay                     replay events to subscriber

  switchover                 switch the roles between provider & subscriber
  compare [TBL ...]          compare table contents on both sides
  repair [TBL ...]           repair data on subscriber
  copy                       full copy of table, internal cmd
  subscriber check           compare table structure on both sides
  subscriber fkeys           print out fkey drop/create commands
  subscriber resync TBL ...  do full copy again
  subscriber register        attaches subscriber to queue (also done by replay)
  subscriber unregister      detach subscriber from queue
"""

class Londiste(skytools.DBScript):
    def __init__(self, args):
        skytools.DBScript.__init__(self, 'londiste', args)

        if self.options.rewind or self.options.reset:
            self.script = Replicator(args)
            return

        if len(self.args) < 2:
            print "need command"
            sys.exit(1)
        cmd = self.args[1]

        if cmd =="provider":
            script = ProviderSetup(args)
        elif cmd == "subscriber":
            script = SubscriberSetup(args)
        elif cmd == "replay":
            method = self.cf.get('method', 'direct')
            if method == 'direct':
                script = Replicator(args)
            elif method == 'file_write':
                script = FileWrite(args)
            elif method == 'file_write':
                script = FileWrite(args)
            else:
                print "unknown method, quitting"
                sys.exit(1)
        elif cmd == "copy":
            script = CopyTable(args)
        elif cmd == "compare":
            script = Comparator(args)
        elif cmd == "repair":
            script = Repairer(args)
        elif cmd == "upgrade":
            script = UpgradeV2(args)
        else:
            print "Unknown command '%s', use --help for help" % cmd
            sys.exit(1)

        self.script = script

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
        g.add_option("--rewind", action="store_true",
                help = "replay: sync queue pos with subscriber")
        g.add_option("--reset", action="store_true",
                help = "replay: forget queue pos on subscriber")
        p.add_option_group(g)

        return p

if __name__ == '__main__':
    script = Londiste(sys.argv[1:])
    script.start()

