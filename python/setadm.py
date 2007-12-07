#! /usr/bin/env python

import sys, optparse, skytools

from pgq.setconsumer import MemberInfo, NodeInfo


class MemberInfo:
    def __init__(self, row):
        self.name = row['node_name']
        self.location = row['node_location']
        self.dead = row['dead']

class SetInfo:
    def __init__(self, set_name, info_row, member_rows):
        self.root_info = info_row
        self.set_name = set_name
        self.member_map = {}
        self.root_name = info_row['node_name']
        self.root_type = info_row['node_type']
        self.global_watermark = info_row['global_watermark']

        for r in member_rows:
            n = MemberInfo(r)
            self.member_map[n.name] = n

    def get_member(self, name):
        return self.member_map.get(name)

command_usage = """
%prog [options] INI CMD [subcmd args]

commands:
"""

class SetAdmin(skytools.DBScript):
    root_name = None
    root_info = None
    member_map = {}
    set_name = None

    def init_optparse(self, parser = None):
        p = skytools.DBScript.init_optparse(self, parser)
        p.set_usage(command_usage.strip())

        g = optparse.OptionGroup(p, "actual setadm options")
        g.add_option("--connstr", action="store_true",
                     help = "add: ignore table differences, repair: ignore lag")
        g.add_option("--provider",
                     help = "add: ignore table differences, repair: ignore lag")
        p.add_option_group(g)
        return p

    def work(self):
        self.set_single_loop(1)

        self.set_name = self.cf.get('set_name')

        if self.is_cmd("init-root", 2):
            self.init_node("root", self.args[2], self.args[3])
        elif self.is_cmd("init-branch", 2):
            self.init_node("branch", self.args[2], self.args[3])
        elif self.is_cmd("init-leaf", 2):
            self.init_node("leaf", self.args[2], self.args[3])
        else:
            self.log.info("need command")

    def is_cmd(self, name, argcnt):
        if len(self.args) < 2:
            return False
        if self.args[1] != name:
            return False
        if len(self.args) != argcnt + 2:
            self.log.error("cmd %s needs %d args" % (name, argcnt))
            sys.exit(1)
        return True

    def init_node(self, node_type, node_name, node_location):
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
        
        worker_name = "%s_%s_worker" % (self.set_name, node_name)

        # register member
        if node_type in ('root', 'combined-root'):
            global_watermark = None
            combined_set = None
            provider_name = None
            self.exec_sql(db, "select pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, node_name, node_location])
            self.exec_sql(db, "select pgq_set.create_node(%s, %s, %s, %s, %s, %s, %s)",
                          [self.set_name, node_type, node_name, worker_name, provider_name, global_watermark, combined_set])
        else:
            root_db = self.find_root_db()
            set = self.load_root_info(root_db)

            # check if member already exists
            if set.get_member(node_name) is not None:
                self.log.error("Node '%s' already exists" % node_name)
                sys.exit(1)

            global_watermark = set.global_watermark
            combined_set = None
            provider_name = self.options.provider

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
            provider_db = self.get_database('provider_db', connstr = provider.location)
            self.exec_sql(provider_db, "select pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, node_name, node_location])
            self.exec_sql(provider_db, "select pgq_set.subscribe_node(%s, %s, %s)",
                          [self.set_name, node_name, worker_name])
            provider_db.commit()

            # initialize node itself
            self.exec_sql(db, "select pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, node_name, node_location])
            self.exec_sql(db, "select pgq_set.add_member(%s, %s, %s, false)",
                          [self.set_name, provider_name, provider.location])
            self.exec_sql(db, "select pgq_set.create_node(%s, %s, %s, %s, %s, %s, %s)",
                          [self.set_name, node_type, node_name, worker_name, provider_name,
                           global_watermark, combined_set])
            db.commit()

            


        self.log.info("Done")

    def find_root_db(self):
        db = self.get_database('root_db')

        while 1:
            # query current status
            res = self.exec_query(db, "select * from pgq_set.get_node_info(%s)", [self.set_name])
            info = res[0]
            type = info['node_type']
            if type is None:
                self.log.info("Root node not initialized?")
                sys.exit(1)

            # configured db may not be root anymore, walk upwards then
            if type in ('root', 'combined-root'):
                db.commit()
                return db

            self.close_connection()
            loc = info['provider_location']
            if loc is None:
                self.log.info("Sub node provider not initialized?")
                sys.exit(1)

            # walk upwards
            db = self.get_database('root_db', connstr = loc)

    def load_root_info(self, db):
        res = self.exec_query(db, "select * from pgq_set.get_node_info(%s)", [self.set_name])
        info = res[0]

        q = "select * from pgq_set.get_member_info(%s)"
        node_list = self.exec_query(db, q, [self.set_name])

        db.commit()

        return SetInfo(self.set_name, info, node_list)

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

    def install_code(self, db):
        objs = [
            skytools.DBLanguage("plpgsql"),
            skytools.DBFunction("txid_current_snapshot", 0, sql_file="txid.sql"),
            skytools.DBSchema("pgq", sql_file="pgq.sql"),
            skytools.DBSchema("pgq_ext", sql_file="pgq_ext.sql"),
            skytools.DBSchema("pgq_set", sql_file="pgq_set.sql"),
        ]
        skytools.db_install(db.cursor(), objs, self.log.debug)
        db.commit()

if __name__ == '__main__':
    script = SetAdmin('set_admin', sys.argv[1:])
    script.start()

