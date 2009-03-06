#! /usr/bin/env python

"""Londiste setup and sanity checker.
"""

import sys, os, skytools

from pgq.cascade.admin import CascadeAdmin

__all__ = ['LondisteSetup']

class LondisteSetup(CascadeAdmin):
    """Londiste-specific admin commands."""
    initial_db_name = 'node_db'
    extra_objs = [ skytools.DBSchema("londiste", sql_file="londiste.sql") ]
    provider_location = None
    def __init__(self, args):
        """Londiste setup init."""
        CascadeAdmin.__init__(self, 'londiste', 'db', args, worker_setup = True)

        # compat
        self.queue_name = self.cf.get('pgq_queue_name', '')
        # real
        if not self.queue_name:
            self.queue_name = self.cf.get('queue_name')

        self.set_name = self.queue_name

    def connection_setup(self, dbname, db):
        if dbname == 'db':
            curs = db.cursor()
            curs.execute("set session_replication_role = 'replica'")
            db.commit()

    def init_optparse(self, parser=None):
        """Add londiste switches to cascadeadmin ones."""

        p = CascadeAdmin.init_optparse(self, parser)
        p.add_option("--expect-sync", action="store_true", dest="expect_sync",
                    help = "no copy needed", default=False)
        p.add_option("--skip-truncate", action="store_true", dest="skip_truncate",
                    help = "dont delete old data", default=False)
        p.add_option("--force", action="store_true",
                    help="force", default=False)
        p.add_option("--all", action="store_true",
                    help="include all tables", default=False)
        p.add_option("--create", action="store_true",
                    help="include all tables", default=False)
        p.add_option("--create-only",
                    help="pkey,fkeys,indexes")
        return p

    def extra_init(self, node_type, node_db, provider_db):
        """Callback from CascadeAdmin init."""
        if not provider_db:
            return
        pcurs = provider_db.cursor()
        ncurs = node_db.cursor()

        # sync tables
        q = "select table_name from londiste.get_table_list(%s)"
        pcurs.execute(q, [self.set_name])
        for row in pcurs.fetchall():
            tbl = row['table_name']
            q = "select * from londiste.global_add_table(%s, %s)"
            ncurs.execute(q, [self.set_name, tbl])

        # sync seqs
        q = "select seq_name, last_value from londiste.get_seq_list(%s)"
        pcurs.execute(q, [self.set_name])
        for row in pcurs.fetchall():
            seq = row['seq_name']
            val = row['last_value']
            q = "select * from londiste.global_update_seq(%s, %s, %s)"
            ncurs.execute(q, [self.set_name, seq, val])

        # done
        node_db.commit()
        provider_db.commit()

    def cmd_add_table(self, *args):
        """Attach table(s) to local node."""

        dst_db = self.get_database('db')
        dst_curs = dst_db.cursor()
        src_db = self.get_provider_db()
        src_curs = src_db.cursor()

        src_tbls = self.fetch_set_tables(src_curs)
        dst_tbls = self.fetch_set_tables(dst_curs)
        src_db.commit()
        self.sync_table_list(dst_curs, src_tbls, dst_tbls)
        dst_db.commit()

        # dont check for exist/not here (root handling)
        problems = False
        for tbl in args:
            tbl = skytools.fq_name(tbl)
            if (tbl in src_tbls) and not src_tbls[tbl]:
                self.log.error("Table %s does not exist on provider, need to switch to different provider" % tbl)
                problems = True
        if problems:
            self.log.error("Problems, canceling operation")
            sys.exit(1)

        # pick proper create flags
        create = self.options.create_only
        if not create and self.options.create:
            create = 'full'

        fmap = {
            "full": skytools.T_ALL,
            "pkey": skytools.T_PKEY,
        }
        create_flags = 0
        if create:
            for f in create.split(','):
                if f not in fmap:
                    raise Exception("bad --create-only flag: " + f)
            create_flags += fmap[f]

        # seems ok
        for tbl in args:
            tbl = skytools.fq_name(tbl)
            self.add_table(src_db, dst_db, tbl, create_flags)

    def add_table(self, src_db, dst_db, tbl, create_flags):
        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()
        tbl_exists = skytools.exists_table(dst_curs, tbl)
        if create_flags:
            if tbl_exists:
                self.log.info('Table %s already exist, not touching' % tbl)
            else:
                if not skytools.exists_table(src_curs, tbl):
                    # table not present on provider - nowhere to get the DDL from
                    self.log.warning('Table "%s" missing on provider, skipping' % tbl)
                    return
                s = skytools.TableStruct(src_curs, tbl)
                src_db.commit()
                s.create(dst_curs, create_flags, log = self.log)
        elif not tbl_exists:
            self.log.warning('Table "%s" missing on subscriber, use --create if necessary' % tbl)
            return

        q = "select * from londiste.local_add_table(%s, %s)"
        self.exec_cmd(dst_curs, q, [self.set_name, tbl])
        dst_db.commit()
    
    def sync_table_list(self, dst_curs, src_tbls, dst_tbls):
        for tbl in src_tbls.keys():
            q = "select * from londiste.global_add_table(%s, %s)"
            if tbl not in dst_tbls:
                self.log.info("Table %s info missing from subscriber, adding" % tbl)
                self.exec_cmd(dst_curs, q, [self.set_name, tbl])
                dst_tbls[tbl] = False
        for tbl in dst_tbls.keys():
            q = "select * from londiste.global_remove_table(%s, %s)"
            if tbl not in src_tbls:
                self.log.info("Table %s gone but exists on subscriber, removing")
                self.exec_cmd(dst_curs, q, [self.set_name, tbl])
                del dst_tbls[tbl]

    def fetch_set_tables(self, curs):
        q = "select table_name, local from londiste.get_table_list(%s)"
        curs.execute(q, [self.set_name])
        res = {}
        for row in curs.fetchall():
            res[row[0]] = row[1]
        return res

    def cmd_remove_table(self, *args):
        """Detach table(s) from local node."""
        q = "select * from londiste.local_remove_table(%s, %s)"
        db = self.get_database('db')
        self.exec_cmd_many(db, q, [self.set_name], args)

    def cmd_add_seq(self, *args):
        """Attach seqs(s) to local node."""
        dst_db = self.get_database('db')
        dst_curs = dst_db.cursor()
        src_db = self.get_provider_db()
        src_curs = src_db.cursor()

        src_seqs = self.fetch_seqs(src_curs)
        dst_seqs = self.fetch_seqs(dst_curs)
        src_db.commit()
        self.sync_seq_list(dst_curs, src_seqs, dst_seqs)
        dst_db.commit()

        # pick proper create flags
        create = self.options.create_only
        if not create and self.options.create:
            create = 'full'

        fmap = {
            "full": skytools.T_SEQUENCE,
        }
        create_flags = 0
        if create:
            for f in create.split(','):
                if f not in fmap:
                    raise Exception("bad --create-only flag: " + f)
            create_flags += fmap[f]

        # seems ok
        for seq in args:
            seq = skytools.fq_name(seq)
            self.add_seq(src_db, dst_db, seq, create_flags)
        dst_db.commit()

    def add_seq(self, src_db, dst_db, seq, create_flags):
        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()
        seq_exists = skytools.exists_sequence(dst_curs, seq)
        if create_flags:
            if seq_exists:
                self.log.info('Sequence %s already exist, not creating' % seq)
            else:
                if not skytools.exists_sequence(src_curs, seq):
                    # sequence not present on provider - nowhere to get the DDL from
                    self.log.warning('Sequence "%s" missing on provider, skipping' % seq)
                    return
                s = skytools.SeqStruct(src_curs, seq)
                src_db.commit()
                s.create(dst_curs, create_flags, log = self.log)
        elif not seq_exists:
            self.log.warning('Sequence "%s" missing on subscriber, use --create if necessary' % seq)
            return

        q = "select * from londiste.local_add_seq(%s, %s)"
        self.exec_cmd(dst_curs, q, [self.set_name, seq])

    def fetch_seqs(self, curs):
        q = "select seq_name, last_value, local from londiste.get_seq_list(%s)"
        curs.execute(q, [self.set_name])
        res = {}
        for row in curs.fetchall():
            res[row[0]] = row
        return res

    def sync_seq_list(self, dst_curs, src_seqs, dst_seqs):
        for seq in src_seqs.keys():
            q = "select * from londiste.global_update_seq(%s, %s, %s)"
            if seq not in dst_seqs:
                self.log.info("Sequence %s info missing from subscriber, adding")
                self.exec_cmd(dst_curs, q, [self.set_name, seq, src_seqs[seq]['last_value']])
                tmp = src_seqs[seq].copy()
                tmp['local'] = False
                dst_seqs[seq] = tmp
        for seq in dst_seqs.keys():
            q = "select * from londiste.global_remove_seq(%s, %s)"
            if seq not in src_seqs:
                self.log.info("Sequence %s gone but exists on subscriber, removing")
                self.exec_cmd(dst_curs, q, [self.set_name, seq])
                del dst_seqs[seq]

    def cmd_remove_seq(self, *args):
        """Detach seqs(s) from local node."""
        q = "select * from londiste.local_remove_seq(%s, %s)"
        db = self.get_database('db')
        self.exec_cmd_many(db, q, [self.set_name], args)

    def cmd_resync(self, *args):
        """Reload data from provider node.."""
        # fixme
        q = "select * from londiste.node_resync_table(%s, %s)"
        db = self.get_database('db')
        self.exec_cmd_many(db, q, [self.set_name], args)

    def cmd_tables(self):
        """Show attached tables."""
        q = "select table_name, local, merge_state from londiste.get_table_list(%s)"
        db = self.get_database('db')
        self.display_table(db, "Tables on node", q, [self.set_name])

    def cmd_seqs(self):
        """Show attached seqs."""
        q = "select seq_name, local, last_value from londiste.get_seq_list(%s)"
        db = self.get_database('db')
        self.display_table(db, "Sequences on node", q, [self.set_name])

    def cmd_missing(self):
        """Show missing tables on local node."""
        # fixme
        q = "select * from londiste.node_show_missing(%s)"
        db = self.get_database('db')
        self.display_table(db, "Missing objects on node", q, [self.set_name])

    def cmd_check(self):
        """TODO: check if structs match"""
        pass
    def cmd_fkeys(self):
        """TODO: show removed fkeys."""
        pass
    def cmd_triggers(self):
        """TODO: show removed triggers."""
        pass

    def cmd_execute(self, *files):
        db = self.get_database('db')
        curs = db.cursor()
        for fn in files:
            fname = os.path.basename(fn)
            sql = open(fn, "r").read()
            q = "select * from londiste.execute_start(%s, %s, %s, true)"
            self.exec_cmd(db, q, [self.queue_name, fname, sql], commit = False)
            for stmt in skytools.parse_statements(sql):
                curs.execute(stmt)
            q = "select * from londiste.execute_finish(%s, %s)"
            self.exec_cmd(db, q, [self.queue_name, fname], commit = False)
        db.commit()

    def get_provider_db(self):
        if not self.provider_location:
            db = self.get_database('db')
            q = 'select * from pgq_node.get_node_info(%s)'
            res = self.exec_cmd(db, q, [self.queue_name], quiet = True)
            self.provider_location = res[0]['provider_location']
        return self.get_database('provider_db', connstr = self.provider_location)

#
# Old commands
#

#class LondisteSetup_tmp(LondisteSetup):
#
#    def find_missing_provider_tables(self, pattern='*'):
#        src_db = self.get_database('provider_db')
#        src_curs = src_db.cursor()
#        q = """select schemaname || '.' || tablename as full_name from pg_tables
#                where schemaname not in ('pgq', 'londiste', 'pg_catalog', 'information_schema')
#                  and schemaname !~ 'pg_.*'
#                  and (schemaname || '.' || tablename) ~ %s
#                except select table_name from londiste.provider_get_table_list(%s)"""
#        src_curs.execute(q, [glob2regex(pattern), self.queue_name])
#        rows = src_curs.fetchall()
#        src_db.commit()
#        list = []
#        for row in rows:
#            list.append(row[0])
#        return list
#                
#    def admin(self):
#        cmd = self.args[2]
#        if cmd == "tables":
#            self.subscriber_show_tables()
#        elif cmd == "missing":
#            self.subscriber_missing_tables()
#        elif cmd == "add":
#            self.subscriber_add_tables(self.args[3:])
#        elif cmd == "remove":
#            self.subscriber_remove_tables(self.args[3:])
#        elif cmd == "resync":
#            self.subscriber_resync_tables(self.args[3:])
#        elif cmd == "register":
#            self.subscriber_register()
#        elif cmd == "unregister":
#            self.subscriber_unregister()
#        elif cmd == "install":
#            self.subscriber_install()
#        elif cmd == "check":
#            self.check_tables(self.get_provider_table_list())
#        elif cmd in ["fkeys", "triggers"]:
#            self.collect_meta(self.get_provider_table_list(), cmd, self.args[3:])
#        elif cmd == "seqs":
#            self.subscriber_list_seqs()
#        elif cmd == "add-seq":
#            self.subscriber_add_seq(self.args[3:])
#        elif cmd == "remove-seq":
#            self.subscriber_remove_seq(self.args[3:])
#        elif cmd == "restore-triggers":
#            self.restore_triggers(self.args[3], self.args[4:])
#        else:
#            self.log.error('bad subcommand: ' + cmd)
#            sys.exit(1)
#
#    def collect_meta(self, table_list, meta, args):
#        """Display fkey/trigger info."""
#
#        if args == []:
#            args = ['pending', 'active']
#            
#        field_map = {'triggers': ['table_name', 'trigger_name', 'trigger_def'],
#                     'fkeys': ['from_table', 'to_table', 'fkey_name', 'fkey_def']}
#        
#        query_map = {'pending': "select %s from londiste.subscriber_get_table_pending_%s(%%s)",
#                     'active' : "select %s from londiste.find_table_%s(%%s)"}
#
#        table_list = self.clean_subscriber_tables(table_list)
#        if len(table_list) == 0:
#            self.log.info("No tables, no fkeys")
#            return
#
#        dst_db = self.get_database('subscriber_db')
#        dst_curs = dst_db.cursor()
#
#        for which in args:
#            union_list = []
#            fields = field_map[meta]
#            q = query_map[which] % (",".join(fields), meta)
#            for tbl in table_list:
#                union_list.append(q % skytools.quote_literal(tbl))
#
#            # use union as fkey may appear in duplicate
#            sql = " union ".join(union_list) + " order by 1"
#            desc = "%s %s" % (which, meta)
#            self.display_table(desc, dst_curs, fields, sql)
#        dst_db.commit()
#
#    def check_tables(self, table_list):
#        src_db = self.get_database('provider_db')
#        src_curs = src_db.cursor()
#        dst_db = self.get_database('subscriber_db')
#        dst_curs = dst_db.cursor()
#
#        failed = 0
#        for tbl in table_list:
#            self.log.info('Checking %s' % tbl)
#            if not skytools.exists_table(src_curs, tbl):
#                self.log.error('Table %s missing from provider side' % tbl)
#                failed += 1
#            elif not skytools.exists_table(dst_curs, tbl):
#                self.log.error('Table %s missing from subscriber side' % tbl)
#                failed += 1
#            else:
#                failed += self.check_table_columns(src_curs, dst_curs, tbl)
#
#        src_db.commit()
#        dst_db.commit()
#
#        return failed
#
#    def check_table_columns(self, src_curs, dst_curs, tbl):
#        src_colrows = find_column_types(src_curs, tbl)
#        dst_colrows = find_column_types(dst_curs, tbl)
#
#        src_cols = make_type_string(src_colrows)
#        dst_cols = make_type_string(dst_colrows)
#        if src_cols.find('k') < 0:
#            self.log.error('provider table %s has no primary key (%s)' % (
#                             tbl, src_cols))
#            return 1
#        if dst_cols.find('k') < 0:
#            self.log.error('subscriber table %s has no primary key (%s)' % (
#                             tbl, dst_cols))
#            return 1
#
#        if src_cols != dst_cols:
#            self.log.warning('table %s structure is not same (%s/%s)'\
#                 ', trying to continue' % (tbl, src_cols, dst_cols))
#
#        err = 0
#        for row in src_colrows:
#            found = 0
#            for row2 in dst_colrows:
#                if row2['name'] == row['name']:
#                    found = 1
#                    break
#            if not found:
#                err = 1
#                self.log.error('%s: column %s on provider not on subscriber'
#                                    % (tbl, row['name']))
#            elif row['type'] != row2['type']:
#                err = 1
#                self.log.error('%s: pk different on column %s'
#                                    % (tbl, row['name']))
#
#        return err
#
#    def find_missing_subscriber_tables(self, pattern='*'):
#        src_db = self.get_database('subscriber_db')
#        src_curs = src_db.cursor()
#        q = """select schemaname || '.' || tablename as full_name from pg_tables
#                where schemaname not in ('pgq', 'londiste', 'pg_catalog', 'information_schema')
#                  and schemaname !~ 'pg_.*'
#                  and schemaname || '.' || tablename ~ %s
#                except select table_name from londiste.provider_get_table_list(%s)"""
#        src_curs.execute(q, [glob2regex(pattern), self.queue_name])
#        rows = src_curs.fetchall()
#        src_db.commit()
#        list = []
#        for row in rows:
#            list.append(row[0])
#        return list
#
