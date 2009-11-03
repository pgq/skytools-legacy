#! /usr/bin/env python

"""Londiste setup and sanity checker.
"""

import sys, os, re, skytools

from pgq.cascade.admin import CascadeAdmin
from skytools.scripting import UsageError

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

    def connection_hook(self, dbname, db):
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
        p.add_option("--copy-condition", dest="copy_condition",
                help = "copy: where expression")
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

        args = self.expand_arg_list(dst_db, 'r', False, args)

        # dont check for exist/not here (root handling)
        problems = False
        for tbl in args:
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
                schema = skytools.fq_name_parts(tbl)[0]
                if not skytools.exists_schema(dst_curs, schema):
                    q = "create schema %s" % skytools.quote_ident(schema)
                    dst_curs.execute(q)
                s = skytools.TableStruct(src_curs, tbl)
                src_db.commit()
                s.create(dst_curs, create_flags, log = self.log)
        elif not tbl_exists:
            self.log.warning('Table "%s" missing on subscriber, use --create if necessary' % tbl)
            return

        # actual table registration
        q = "select * from londiste.local_add_table(%s, %s)"
        self.exec_cmd(dst_curs, q, [self.set_name, tbl])
        if self.options.expect_sync:
            q = "select * from londiste.local_set_table_state(%s, %s, NULL, 'ok')"
            self.exec_cmd(dst_curs, q, [self.set_name, tbl])
        if self.options.copy_condition:
            q = "select * from londiste.local_set_table_attrs(%s, %s, %s)"
            attrs = {'copy_condition': self.options.copy_condition}
            enc_attrs = skytools.db_urlencode(attrs)
            self.exec_cmd(dst_curs, q, [self.set_name, tbl, enc_attrs])
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
        db = self.get_database('db')
        args = self.expand_arg_list(db, 'r', True, args)
        q = "select * from londiste.local_remove_table(%s, %s)"
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

        args = self.expand_arg_list(dst_db, 'S', False, args)

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
                self.log.info("Sequence %s info missing from subscriber, adding" % seq)
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
        args = self.expand_arg_list(db, 'S', True, args)
        self.exec_cmd_many(db, q, [self.set_name], args)

    def cmd_resync(self, *args):
        """Reload data from provider node.."""
        db = self.get_database('db')
        args = self.expand_arg_list(db, 'r', True, args)
        q = "select * from londiste.local_set_table_state(%s, %s, null, null)"
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
        q = "select * from londiste.local_show_missing(%s)"
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
            res = self.exec_cmd(db, q, [self.queue_name, fname, sql], commit = False)
            ret = res[0]['ret_code']
            if ret >= 300:
                self.log.warning("Skipping execution of '%s'" % fname)
                continue
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


    def expand_arg_list(self, db, kind, existing, args):
        curs = db.cursor()

        if kind == 'S':
            q1 = "select seq_name, local from londiste.get_seq_list(%s) where local"
        elif kind == 'r':
            q1 = "select table_name, local from londiste.get_table_list(%s) where local"
        else:
            raise Exception("bug")
        q2 = "select obj_name from londiste.local_show_missing(%%s) where obj_kind = '%s'" % kind

        lst_exists = []
        map_exists = {}
        curs.execute(q1, [self.set_name])
        for row in curs.fetchall():
            lst_exists.append(row[0])
            map_exists[row[0]] = 1

        lst_missing = []
        map_missing = {}
        curs.execute(q2, [self.set_name])
        for row in curs.fetchall():
            lst_missing.append(row[0])
            map_missing[row[0]] = 1

        db.commit()

        if not args and self.options.all:
            if existing:
                return lst_exists
            else:
                return lst_missing

        if existing:
            res = self.solve_globbing(args, lst_exists, map_exists, map_missing)
        else:
            res = self.solve_globbing(args, lst_missing, map_missing, map_exists)
        return res


    def solve_globbing(self, args, full_list, full_map, reverse_map):
        def glob2regex(s):
            s = s.replace('.', '[.]').replace('?', '.').replace('*', '.*')
            return '^%s$' % s

        res_map = {}
        res_list = []
        err = 0
        for a in args:
            if a.find('*') >= 0 or a.find('?') >= 0:
                if a.find('.') < 0:
                    a = 'public.' + a
                rc = re.compile(glob2regex(a))
                for x in full_list:
                    if rc.match(x):
                        if not x in res_map:
                            res_map[x] = 1
                            res_list.append(x)
            else:
                a = skytools.fq_name(a)
                if a in res_map:
                    continue
                elif a in full_map:
                    res_list.append(a)
                    res_map[a] = 1
                elif a in reverse_map:
                    self.log.info("%s already processed" % a)
                else:
                    self.log.warning("%s not available" % a)
                    err = 1
        if err:
            raise UsageError("Cannot proceed")
        return res_list

