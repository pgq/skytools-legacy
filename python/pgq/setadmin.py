#! /usr/bin/env python

import sys, time, optparse, skytools

from pgq.setinfo import *

__all__ = ['SetAdmin']

command_usage = """\
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
  status                Show set state
  members               Show members in set
  rename-node OLD NEW   Rename a node
  change-provider NODE NEWSRC   
  pause NODE
  resume NODE

  switchover NEWROOT
  failover NEWROOT
  tag-dead NODE ..      Tag node as dead
  tag-alive NODE ..     Tag node as alive
"""

class SetAdmin(skytools.AdminScript):
    set_name = None
    extra_objs = []
    initial_db_name = 'node_db'

    def init_optparse(self, parser = None):
        p = skytools.AdminScript.init_optparse(self, parser)
        p.set_usage(command_usage.strip())

        g = optparse.OptionGroup(p, "actual setadm options")
        g.add_option("--connstr", action="store_true",
                     help = "initial connect string")
        g.add_option("--provider",
                     help = "init: connect string for provider")
        p.add_option_group(g)
        return p

    def reload(self):
        skytools.AdminScript.reload(self)
        self.set_name = self.cf.get('set_name')

    #
    # Node initialization.
    #

    def cmd_init_root(self, node_name, node_location):
        self.init_node('root', node_name, node_location)

    def cmd_init_branch(self, node_name, node_location):
        if len(args) != 2:
            raise Exception('init-branch needs 2 args')
        self.init_node('branch', node_name, node_location)

    def cmd_init_leaf(self, node_name, node_location):
        self.init_node('leaf', node_name, node_location)

    def init_node(self, node_type, node_name, node_location):
        provider_loc = self.options.provider

        # connect to database
        db = self.get_database("new_node", connstr = node_location)

        # check if code is installed
        self.install_code(db)

        # query current status
        res = self.exec_query(db, "select * from pgq_set.get_node_info(%s)", [self.set_name])
        info = res[0]
        if info['node_type'] is not None:
            self.log.info("Node is already initialized as %s" % info['node_type'])
            return
        
        self.log.info("Initializing node")

        # register member
        if node_type in ('root', 'combined-root'):
            global_watermark = None
            combined_set = None
            provider_name = None
            self.exec_cmd(db, "select * from pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, node_name, node_location])
            self.exec_cmd(db, "select * from pgq_set.create_node(%s, %s, %s, %s, %s, %s)",
                          [self.set_name, node_type, node_name, provider_name, global_watermark, combined_set])
            provider_db = None
        else:
            root_db = self.find_root_db(provider_loc)
            set = self.load_set_info(root_db)

            # check if member already exists
            if set.get_member(node_name) is not None:
                self.log.error("Node '%s' already exists" % node_name)
                sys.exit(1)

            combined_set = None

            provider_db = self.get_database('provider_db', connstr = provider_loc)
            q = "select node_type, node_name from pgq_set.get_node_info(%s)"
            res = self.exec_query(provider_db, q, [self.set_name])
            row = res[0]
            if not row['node_name']:
                raise Exception("provider node not found")
            provider_name = row['node_name']

            # register member on root
            self.exec_cmd(root_db, "select * from pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, node_name, node_location])

            # lookup provider
            provider = set.get_member(provider_name)
            if not provider:
                self.log.error("Node %s does not exist" % provider_name)
                sys.exit(1)

            # register on provider
            self.exec_cmd(provider_db, "select * from pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, node_name, node_location])
            rows = self.exec_cmd(provider_db, "select * from pgq_set.subscribe_node(%s, %s)",
                                 [self.set_name, node_name])
            global_watermark = rows[0]['global_watermark']

            # initialize node itself

            # insert members
            self.exec_cmd(db, "select * from pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, node_name, node_location])
            for m in set.member_map.values():
                self.exec_cmd(db, "select * from pgq_set.add_member(%s, %s, %s, %s)",
                              [self.set_name, m.name, m.location, m.dead])

            # real init
            self.exec_cmd(db, "select * from pgq_set.create_node(%s, %s, %s, %s, %s, %s)",
                          [self.set_name, node_type, node_name, provider_name,
                           global_watermark, combined_set])


        self.extra_init(node_type, db, provider_db)

        self.log.info("Done")

    def extra_init(self, node_type, node_db, provider_db):
        pass

    def find_root_db(self, initial_loc = None):
        if initial_loc:
            loc = initial_loc
        else:
            loc = self.cf.get(self.initial_db_name)

        while 1:
            db = self.get_database('root_db', connstr = loc)


            # query current status
            res = self.exec_query(db, "select * from pgq_set.get_node_info(%s)", [self.set_name])
            info = res[0]
            type = info['node_type']
            if type is None:
                self.log.info("Root node not initialized?")
                sys.exit(1)

            self.log.debug("db='%s' -- type='%s' provider='%s'" % (loc, type, info['provider_location']))
            # configured db may not be root anymore, walk upwards then
            if type in ('root', 'combined-root'):
                db.commit()
                return db

            self.close_database('root_db')
            if loc == info['provider_location']:
                raise Exception("find_root_db: got loop: %s" % loc)
            loc = info['provider_location']
            if loc is None:
                self.log.error("Sub node provider not initialized?")
                sys.exit(1)

    def install_code(self, db):
        objs = [
            skytools.DBLanguage("plpgsql"),
            skytools.DBFunction("txid_current_snapshot", 0, sql_file="txid.sql"),
            skytools.DBSchema("pgq", sql_file="pgq.sql"),
            skytools.DBSchema("pgq_ext", sql_file="pgq_ext.sql"),
            skytools.DBSchema("pgq_set", sql_file="pgq_set.sql"),
        ]
        objs += self.extra_objs
        skytools.db_install(db.cursor(), objs, self.log)
        db.commit()

    #
    # Print status of whole set.
    #

    def cmd_status(self):
        root_db = self.find_root_db()
        sinf = self.load_set_info(root_db)

        for mname, minf in sinf.member_map.iteritems():
            db = self.get_database('look_db', connstr = minf.location, autocommit = 1)
            curs = db.cursor()
            curs.execute("select * from pgq_set.get_node_info(%s)", [self.set_name])
            node = NodeInfo(self.set_name, curs.fetchone())
            node.load_status(curs)
            self.load_extra_status(curs, node)
            sinf.add_node(node)
            self.close_database('look_db')

        sinf.print_tree()

    def load_extra_status(self, curs, node):
        pass

    #
    # Normal commands.
    #

    def cmd_change_provider(self, node_name, new_provider):
        old_provider = None

        self.load_local_info()
        node_location = self.set_info.get_member(node_name).location
        node_db = self.get_node_database(node_name)
        node_set_info = self.load_set_info(node_db)
        node = node_set_info.local_node
        old_provider = node.provider_node

        if old_provider == new_provider:
            self.log.info("Node %s has already %s as provider" % (node_name, new_provider))

        # pause target node
        self.pause_node(node_name)

        # reload node info
        node_set_info = self.load_set_info(node_db)
        node = node_set_info.local_node

        # subscribe on new provider
        q = "select * from pgq_set.add_member(%s, %s, %s, false)"
        self.node_cmd(new_provider, q, [self.set_name, node_name, node_location])
        q = 'select * from pgq_set.subscribe_node(%s, %s, %s)'
        self.node_cmd(new_provider, q, [self.set_name, node_name, node.completed_tick])

        # change provider on node
        q = 'select * from pgq_set.change_provider(%s, %s)'
        self.node_cmd(node_name, q, [self.set_name, new_provider])

        # unsubscribe from old provider
        q = "select * from pgq_set.unsubscribe_node(%s, %s)"
        self.node_cmd(old_provider, q, [self.set_name, node_name])

        # resume node
        self.resume_node(node_name)

    def cmd_rename_node(self, old_name, new_name):

        self.load_local_info()

        root_db = self.find_root_db()

        # pause target node
        self.pause_node(old_name)
        node = self.load_node_info(old_name)
        provider_node = node.provider_node
        subscriber_list = self.get_node_subscriber_list(old_name)


        # create copy of member info / subscriber+queue info
        step1 = 'select * from pgq_set.rename_node_step1(%s, %s, %s)'
        # rename node itself, drop copies
        step2 = 'select * from pgq_set.rename_node_step2(%s, %s, %s)'

        # step1
        self.exec_cmd(root_db, step1, [self.set_name, old_name, new_name])
        self.node_cmd(provider_node, step1, [self.set_name, old_name, new_name])
        self.node_cmd(old_name, step1, [self.set_name, old_name, new_name])
        for child in subscriber_list:
            self.node_cmd(child, step1, [self.set_name, old_name, new_name])

        # step1
        self.node_cmd(old_name, step2, [self.set_name, old_name, new_name])
        self.node_cmd(provider_node, step1, [self.set_name, old_name, new_name])
        for child in subscriber_list:
            self.node_cmd(child, step2, [self.set_name, old_name, new_name])
        self.exec_cmd(root_db, step2, [self.set_name, old_name, new_name])

        # resume node
        self.resume_node(old_name)

    def switchover_nonroot(self, old_node, new_node):
        # see if we need to change new nodes' provider
        tmp_node = new_node
        while 1:
            if tmp_node.is_root():
                break
            if tmp_node.name == old_node:
                # yes, old_node is new_nodes provider,
                # switch it around
                self.change_provider(new_node, old_node.parent_node)
                break
        self.change_provider(old_node.name, new_node.name)

    def switchover_root(self, old_node, new_node):
        self.pause_node(old_node.name)
        self.extra_lockdown(old_node)

        self.wait_for_catchup(new_node, old_node)
        self.pause_node(new_node.name)
        self.promote_node(new_node.name)
        self.subscribe_node(new_node.name, old_node.name, tick_pos)
        self.unsubscribe_node(new_node.parent_node, new_node.name)
        self.resume_node(new_node.name)

        # demote & set provider on node
        q = 'select * from pgq_set.demote_root(%s, %s)'
        self.node_cmd(old_node.name, q, [self.set_name, new_node.name])

        self.resume_node(old_node.name)

    def cmd_switchover(self, old_node_name, new_node_name):
        self.load_local_info()
        old_node = self.get_node_info(old_node_name)
        new_node = self.get_node_info(new_node_name)
        if old_node.name == new_node.name:
            self.log.info("same node?")
            return

        if old_node.is_root():
            self.switchover_root(old_node, new_node)
        else:
            self.switchover_nonroot(old_node, new_node)

        # switch subscribers around
        if self.options.all:
            for n in self.get_node_subscriber_list(old_node.name):
                self.change_provider(n, new_node.name)

    def cmd_pause(self, node_name):
        self.load_local_info()
        self.pause_node(node_name)

    def cmd_resume(self, node_name):
        self.load_local_info()
        self.resume_node(node_name)

    def cmd_members(self):
        db = self.get_database(self.initial_db_name)
        q = "select node_name from pgq_set.get_node_info(%s)"
        rows = self.exec_query(db, q, [self.set_name])

        desc = 'Member info on %s:' % rows[0]['node_name']
        q = "select node_name, dead, node_location"\
            " from pgq_set.get_member_info(%s) order by 1"
        self.display_table(db, desc, q, [self.set_name])

    #
    # Shortcuts for operating on nodes.
    #

    def load_local_info(self):
        """fetch set info from local node."""
        db = self.get_database(self.initial_db_name)
        self.set_info = self.load_set_info(db)

    def get_node_database(self, node_name):
        """Connect to node."""
        if node_name == self.set_info.local_node.name:
            db = self.get_database(self.initial_db_name)
        else:
            m = self.set_info.get_member(node_name)
            if not m:
                self.log.error("cannot resolve %s" % node_name)
                sys.exit(1)
            loc = m.location
            db = self.get_database('node.' + node_name, connstr = loc)
        return db

    def close_node_database(self, node_name):
        """Disconnect node's connection."""
        if node_name == self.set_info.local_node.name:
            self.close_database(self.initial_db_name)
        else:
            self.close_database("node." + node_name)

    def node_cmd(self, node_name, sql, args):
        """Execute SQL command on particular node."""
        db = self.get_node_database(node_name)
        return self.exec_cmd(db, sql, args)

    #
    # Various operation on nodes.
    #

    def set_paused(self, db, pause_flag):
        q = "select * from pgq_set.set_node_paused(%s, %s)"
        self.exec_cmd(db, q, [self.set_name, pause_flag])

        self.log.info('Waiting for worker to accept')
        while 1:
            q = "select * from pgq_set.get_node_info(%s)"
            stat = self.exec_query(db, q, [self.set_name])[0]
            if stat['paused'] != pause_flag:
                raise Exception('operation canceled? %s <> %s' % (repr(stat['paused']), repr(pause_flag)))

            if stat['uptodate']:
                break
            time.sleep(1)

        op = pause_flag and "paused" or "resumed"

        self.log.info("Node %s %s" % (stat['node_name'], op))

    def pause_node(self, node_name):
        db = self.get_node_database(node_name)
        self.set_paused(db, True)

    def resume_node(self, node_name):
        db = self.get_node_database(node_name)
        self.set_paused(db, False)

    def subscribe_node(self, target_node, subscriber_node, tick_pos):
        q = "select * from pgq_set.subscribe_node(%s, %s, %s)"
        self.node_cmd(target_node, q, [self.set_name, subscribe_node, tick_pos])

    def unsubscribe_node(self, target_node, subscriber_node):
        q = "select * from pgq_set.unsubscribe_node(%s, %s)"
        self.node_cmd(target_node, q, [self.set_name, subscribe_node])

    def load_node_info(self, node_name):
        db = self.get_node_database(node_name)
        q = "select * from pgq_set.get_node_info(%s)"
        rows = self.exec_query(db, q, [self.set_name])
        return NodeInfo(self.set_name, rows[0])

    def load_set_info(self, db):
        res = self.exec_query(db, "select * from pgq_set.get_node_info(%s)", [self.set_name])
        info = res[0]

        q = "select * from pgq_set.get_member_info(%s)"
        member_list = self.exec_query(db, q, [self.set_name])

        return SetInfo(self.set_name, info, member_list)

    def get_node_subscriber_list(self, node_name):
        q = "select node_name, local_watermark from pgq_set.get_subscriber_info(%s)"
        db = self.get_node_database(node_name)
        rows = self.exec_query(db, q, [self.set_name])
        return [r['node_name'] for r in rows]

if __name__ == '__main__':
    script = SetAdmin('set_admin', sys.argv[1:])
    script.start()

