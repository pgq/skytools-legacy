#! /usr/bin/env python

import sys, optparse, skytools

from pgq.setinfo import *

__all__ = ['SetAdmin']

command_usage = """
%prog [options] INI CMD [subcmd args]

commands:
"""

class SetAdmin(skytools.AdminScript):
    root_name = None
    root_info = None
    member_map = {}
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

    def cmd_init_root(self, args):
        if len(args) != 2:
            raise Exception('init-root needs 2 args')
        self.init_node('root', args[0], args[1])

    def cmd_init_branch(self, args):
        if len(args) != 2:
            raise Exception('init-branch needs 2 args')
        self.init_node('branch', args[0], args[1])

    def cmd_init_leaf(self, args):
        if len(args) != 2:
            raise Exception('init-leaf needs 2 args')
        self.init_node('leaf', args[0], args[1])

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
            self.exec_sql(db, "select pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, node_name, node_location])
            self.exec_sql(db, "select pgq_set.create_node(%s, %s, %s, %s, %s, %s)",
                          [self.set_name, node_type, node_name, provider_name, global_watermark, combined_set])
            provider_db = None
        else:
            root_db = self.find_root_db(provider_loc)
            set = self.load_set_info(root_db)

            # check if member already exists
            if set.get_member(node_name) is not None:
                self.log.error("Node '%s' already exists" % node_name)
                sys.exit(1)

            global_watermark = set.global_watermark
            combined_set = None

            provider_db = self.get_database('provider_db', connstr = provider_loc)
            curs = provider_db.cursor()
            curs.execute("select node_type, node_name from pgq_set.get_node_info(%s)", [self.set_name])
            provider_db.commit()
            row = curs.fetchone()
            if not row:
                raise Exception("provider node not found")
            provider_name = row['node_name']

            # register member on root
            self.exec_sql(root_db, "select pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, node_name, node_location])
            root_db.commit()

            # lookup provider
            provider = set.get_member(provider_name)
            if not provider:
                self.log.error("Node %s does not exist" % provider_name)
                sys.exit(1)

            # register on provider
            self.exec_sql(provider_db, "select pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, node_name, node_location])
            self.exec_sql(provider_db, "select pgq_set.subscribe_node(%s, %s)",
                          [self.set_name, node_name])
            provider_db.commit()

            # initialize node itself
            self.exec_sql(db, "select pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, node_name, node_location])
            self.exec_sql(db, "select pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, provider_name, provider.location])
            self.exec_sql(db, "select pgq_set.create_node(%s, %s, %s, %s, %s, %s)",
                          [self.set_name, node_type, node_name, provider_name,
                           global_watermark, combined_set])
            db.commit()

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


            if 1:
                curs = db.cursor()
                curs.execute("select current_database()")
                n = curs.fetchone()[0]
                self.log.debug("real dbname=%s" % n)
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
                self.log.info("Sub node provider not initialized?")
                sys.exit(1)

    def load_set_info(self, db):
        res = self.exec_query(db, "select * from pgq_set.get_node_info(%s)", [self.set_name])
        info = res[0]

        q = "select * from pgq_set.get_member_info(%s)"
        member_list = self.exec_query(db, q, [self.set_name])

        db.commit()

        return SetInfo(self.set_name, info, member_list)

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

    def cmd_status(self, args):
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

    def cmd_switch(self, node_name, new_provider):
        node_db = self.get_node_database(node_name)
        new_provider_db = self.get_node_database(new_provider)
        node_info = self.load_set_info(node_db)

        # 
        [['node', 'PAUSE']]
        [['new_parent', 'select * from pgq_set.subscribe_node(%(set_name)s, %(node_name)s, %(node_pos)s)']]
        [['node', 'select * from pgq_set.change_provider(%(set_name)s, %(new_provider)s)']]
        [['old_parent', 'select * from pgq_set.unsubscribe_node(%(set_name)s, %(node_name)s, %(node_pos)s)']]
        [['node', 'RESUME']]

    def cmd_promote(self):
        old_root = 'foo'
        new_root = ''
        self.pause_node(old_root)
        ctx = self.load_node_info(old_root)
        [['old-root', 'PAUSE']]
        [['old-root', 'demote, set-provider?']]
        [['new-root', 'wait-for-catch-up']]
        [['new-root', 'pause']]
        [['new-root', 'promote']]
        [['new-root', 'resume']]
        [['old-root', 'resume']]
        [['new_parent', 'select * from pgq_set.subscribe_node(%(set_name)s, %(node_name)s, %(node_pos)s)']]
        [['node', 'select * from pgq_set.change_provider(%(set_name)s, %(new_provider)s)']]
        [['old_parent', 'select * from pgq_set.unsubscribe_node(%(set_name)s, %(node_name)s, %(node_pos)s)']]
        [['node', 'RESUME']]

    def subscribe_node(self, target_node, subscriber_node, tick_pos):
        q = "select * from pgq_set.subscribe_node(%s, %s, %s)"
        self.node_exec(target_node, q, [self.set_name, target_node, tick_pos])

    def unsubscribe_node(self, target_node, subscriber_node, tick_pos):
        q = "select * from pgq_set.subscribe_node(%s, %s, %s)"
        self.node_exec(target_node, q, [self.set_name, target_node, tick_pos])

    def node_cmd(self, node_name, sql, args, commit = True):
        m = self.lookup_member(node_name)
        db = self.get_database('node_'+node_name)
        self.db_cmd(db, sql, args, commit = commit)

    def connect_node(self, node_name):
        sinf = self.get_set_info()
        m = sinf.get_member(node_name)
        loc = m.node_location
        db = self.get_database("node." + node_name, connstr = loc)

    def disconnect_node(self, node_name):
        self.close_database("node." + node_name)

if __name__ == '__main__':
    script = SetAdmin('set_admin', sys.argv[1:])
    script.start()

