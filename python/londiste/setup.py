#! /usr/bin/env python

"""Londiste setup and sanity checker.
"""

import sys, os, re, skytools

from pgq.cascade.admin import CascadeAdmin
from londiste.exec_attrs import ExecAttrs

import londiste.handler

__all__ = ['LondisteSetup']

class LondisteSetup(CascadeAdmin):
    """Londiste-specific admin commands."""
    initial_db_name = 'node_db'
    provider_location = None

    def install_code(self, db):
        self.extra_objs = [
            skytools.DBSchema("londiste", sql_file = 'londiste.sql'),
            skytools.DBFunction("londiste.global_add_table", 2, sql_file = 'londiste.upgrade_2.1_to_3.1.sql'),
        ]
        CascadeAdmin.install_code(self, db)

    def __init__(self, args):
        """Londiste setup init."""
        CascadeAdmin.__init__(self, 'londiste3', 'db', args, worker_setup = True)

        # compat
        self.queue_name = self.cf.get('pgq_queue_name', '')
        # real
        if not self.queue_name:
            self.queue_name = self.cf.get('queue_name')

        self.set_name = self.queue_name

        londiste.handler.load_handler_modules(self.cf)

    def init_optparse(self, parser=None):
        """Add londiste switches to cascadeadmin ones."""

        p = CascadeAdmin.init_optparse(self, parser)
        p.add_option("--expect-sync", action="store_true", dest="expect_sync",
                    help = "no copy needed", default=False)
        p.add_option("--skip-truncate", action="store_true", dest="skip_truncate",
                    help = "dont delete old data", default=False)
        p.add_option("--find-copy-node", action="store_true", dest="find_copy_node",
                help = "add: find table source for copy by walking upwards")
        p.add_option("--copy-node", dest="copy_node",
                help = "add: use NODE as source for initial copy")
        p.add_option("--force", action="store_true",
                    help="force", default=False)
        p.add_option("--all", action="store_true",
                    help="include all tables", default=False)
        p.add_option("--wait-sync", action="store_true",
                help = "add: wait until all tables are in sync"),
        p.add_option("--create", action="store_true",
                    help="create, minimal", default=False)
        p.add_option("--create-full", action="store_true",
                    help="create, full")
        p.add_option("--trigger-flags",
                    help="Set trigger flags (BAIUDLQ)")
        p.add_option("--trigger-arg", action="append",
                    help="Custom trigger arg")
        p.add_option("--no-triggers", action="store_true",
                    help="Custom trigger arg")
        p.add_option("--handler", action="store",
                help="add: Custom handler for table")
        p.add_option("--handler-arg", action="append",
                    help="add: Argument to custom handler")
        p.add_option("--merge-all", action="store_true",
                    help="merge tables from all source queues", default=False)
        p.add_option("--no-merge", action="store_true",
                    help="don't merge tables from source queues", default=False)
        p.add_option("--max-parallel-copy", type = "int",
                    help="max number of parallel copy processes")
        p.add_option("--dest-table",
                    help="add: name for actual table")
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

    def is_root(self):
        return self.queue_info.local_node.type == 'root'

    def cmd_add_table(self, *args):
        """Attach table(s) to local node."""

        self.load_local_info()

        src_db = self.get_provider_db()
        if not self.is_root():
            src_curs = src_db.cursor()
            src_tbls = self.fetch_set_tables(src_curs)
            src_db.commit()

        dst_db = self.get_database('db')
        dst_curs = dst_db.cursor()
        dst_tbls = self.fetch_set_tables(dst_curs)
        if self.is_root():
            src_tbls = dst_tbls
        else:
            self.sync_table_list(dst_curs, src_tbls, dst_tbls)
        dst_db.commit()

        needs_tbl = self.handler_needs_table()
        args = self.expand_arg_list(dst_db, 'r', False, args, needs_tbl)

        # dont check for exist/not here (root handling)
        if not self.is_root() and not self.options.expect_sync and not self.options.find_copy_node:
            problems = False
            for tbl in args:
                tbl = skytools.fq_name(tbl)
                if (tbl in src_tbls) and not src_tbls[tbl]['local']:
                    self.log.error("Table %s does not exist on provider, need to switch to different provider" % tbl)
                    problems = True
            if problems:
                self.log.error("Problems, canceling operation")
                sys.exit(1)

        # pick proper create flags
        if self.options.create_full:
            create_flags = skytools.T_ALL
        elif self.options.create:
            create_flags = skytools.T_TABLE | skytools.T_PKEY
        else:
            create_flags = 0

        # sanity check
        if self.options.dest_table and len(args) > 1:
            self.log.error("--dest-table can be given only for single table")
            sys.exit(1)

        # not implemented
        if self.options.find_copy_node and create_flags != 0:
            self.log.error("--find-copy-node does not work with --create")
            sys.exit(1)

        # seems ok
        for tbl in args:
            self.add_table(src_db, dst_db, tbl, create_flags, src_tbls)

        # wait
        if self.options.wait_sync:
            self.wait_for_sync(dst_db)

    def add_table(self, src_db, dst_db, tbl, create_flags, src_tbls):
        # use full names
        tbl = skytools.fq_name(tbl)
        dest_table = self.options.dest_table or tbl
        dest_table = skytools.fq_name(dest_table)

        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()
        tbl_exists = skytools.exists_table(dst_curs, dest_table)

        if dest_table == tbl:
            desc = tbl
        else:
            desc = "%s(%s)" % (tbl, dest_table)

        if create_flags:
            if tbl_exists:
                self.log.info('Table %s already exist, not touching' % desc)
            else:
                src_dest_table = src_tbls[tbl]['dest_table']
                if not skytools.exists_table(src_curs, src_dest_table):
                    # table not present on provider - nowhere to get the DDL from
                    self.log.warning('Table %s missing on provider, cannot create, skipping' % desc)
                    return
                schema = skytools.fq_name_parts(dest_table)[0]
                if not skytools.exists_schema(dst_curs, schema):
                    q = "create schema %s" % skytools.quote_ident(schema)
                    dst_curs.execute(q)
                s = skytools.TableStruct(src_curs, src_dest_table)
                src_db.commit()

                # create, using rename logic only when necessary
                newname = None
                if src_dest_table != dest_table:
                    newname = dest_table
                s.create(dst_curs, create_flags, log = self.log, new_table_name = newname)

        tgargs = self.build_tgargs()

        attrs = {}

        if self.options.handler:
            attrs['handler'] = self.build_handler(tbl, tgargs, self.options.dest_table)

        if self.options.find_copy_node:
            attrs['copy_node'] = '?'
        elif self.options.copy_node:
            attrs['copy_node'] = self.options.copy_node

        if not self.options.expect_sync:
            if self.options.skip_truncate:
                attrs['skip_truncate'] = 1

        if self.options.max_parallel_copy:
            attrs['max_parallel_copy'] = self.options.max_parallel_copy

        # actual table registration
        args = [self.set_name, tbl, tgargs, None, None]
        if attrs:
            args[3] = skytools.db_urlencode(attrs)
        if dest_table != tbl:
            args[4] = dest_table
        q = "select * from londiste.local_add_table(%s, %s, %s, %s, %s)"
        self.exec_cmd(dst_curs, q, args)
        dst_db.commit()

    def build_tgargs(self):
        """Build trigger args"""
        tgargs = []
        if self.options.trigger_arg:
            tgargs = self.options.trigger_arg
        tgflags = self.options.trigger_flags
        if tgflags:
            tgargs.append('tgflags='+tgflags)
        if self.options.no_triggers:
            tgargs.append('no_triggers')
        if self.options.merge_all:
            tgargs.append('merge_all')
        if self.options.no_merge:
            tgargs.append('no_merge')
        if self.options.expect_sync:
            tgargs.append('expect_sync')
        return tgargs

    def build_handler(self, tbl, tgargs, dest_table=None):
        """Build handler and return handler string"""
        hstr = londiste.handler.create_handler_string(
                self.options.handler, self.options.handler_arg)
        p = londiste.handler.build_handler(tbl, hstr, dest_table)
        p.add(tgargs)
        return hstr

    def handler_needs_table(self):
        if self.options.handler:
            hstr = londiste.handler.create_handler_string(
                            self.options.handler, self.options.handler_arg)
            p = londiste.handler.build_handler('unused.string', hstr, None)
            return p.needs_table()
        return True

    def sync_table_list(self, dst_curs, src_tbls, dst_tbls):
        for tbl in src_tbls.keys():
            q = "select * from londiste.global_add_table(%s, %s)"
            if tbl not in dst_tbls:
                self.log.info("Table %s info missing from subscriber, adding" % tbl)
                self.exec_cmd(dst_curs, q, [self.set_name, tbl])
                dst_tbls[tbl] = {'local': False, 'dest_table': tbl}
        for tbl in dst_tbls.keys():
            q = "select * from londiste.global_remove_table(%s, %s)"
            if tbl not in src_tbls:
                self.log.info("Table %s gone but exists on subscriber, removing")
                self.exec_cmd(dst_curs, q, [self.set_name, tbl])
                del dst_tbls[tbl]

    def fetch_set_tables(self, curs):
        q = "select table_name, local, "\
            " coalesce(dest_table, table_name) as dest_table "\
            " from londiste.get_table_list(%s)"
        curs.execute(q, [self.set_name])
        res = {}
        for row in curs.fetchall():
            res[row[0]] = row
        return res

    def cmd_remove_table(self, *args):
        """Detach table(s) from local node."""
        db = self.get_database('db')
        args = self.expand_arg_list(db, 'r', True, args)
        q = "select * from londiste.local_remove_table(%s, %s)"
        self.exec_cmd_many(db, q, [self.set_name], args)

    def cmd_change_handler(self, tbl):
        """Change handler (table_attrs) of the replicated table."""

        self.load_local_info()

        tbl = skytools.fq_name(tbl)

        db = self.get_database('db')
        curs = db.cursor()
        q = "select table_attrs, dest_table "\
            " from londiste.get_table_list(%s) "\
            " where table_name = %s and local"
        curs.execute(q, [self.set_name, tbl])
        if curs.rowcount == 0:
            self.log.error("Table %s not found on this node" % tbl)
            sys.exit(1)

        attrs, dest_table = curs.fetchone()
        attrs = skytools.db_urldecode(attrs or '')
        old_handler = attrs.get('handler')

        tgargs = self.build_tgargs()
        if self.options.handler:
            new_handler = self.build_handler(tbl, tgargs, dest_table)
        else:
            new_handler = None

        if old_handler == new_handler:
            self.log.info("Handler is already set to desired value, nothing done")
            sys.exit(0)

        if new_handler:
            attrs['handler'] = new_handler
        elif 'handler' in attrs:
            del attrs['handler']

        args = [self.set_name, tbl, tgargs, None]
        if attrs:
            args[3] = skytools.db_urlencode(attrs)

        q = "select * from londiste.local_change_handler(%s, %s, %s, %s)"
        self.exec_cmd(curs, q, args)
        db.commit()

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
        if self.options.create_full:
            create_flags = skytools.T_SEQUENCE
        elif self.options.create:
            create_flags = skytools.T_SEQUENCE
        else:
            create_flags = 0

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
        """Reload data from provider node."""
        db = self.get_database('db')
        args = self.expand_arg_list(db, 'r', True, args)
        q = "select * from londiste.local_set_table_state(%s, %s, null, null)"
        self.exec_cmd_many(db, q, [self.set_name], args)

    def cmd_tables(self):
        """Show attached tables."""
        q = """select table_name, merge_state, table_attrs
        from londiste.get_table_list(%s) where local
        order by table_name"""
        db = self.get_database('db')
        def show_attr(a):
            if a:
                return skytools.db_urldecode(a)
            return ''
        self.display_table(db, "Tables on node", q, [self.set_name],
                           fieldfmt = {'table_attrs': show_attr})

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

    def cmd_show_handlers(self, *args):
        """Show help about handlers."""
        londiste.handler.show(args)

    def cmd_execute(self, *files):
        db = self.get_database('db')
        curs = db.cursor()

        tables = self.fetch_set_tables(curs)
        seqs = self.fetch_seqs(curs)

        # generate local maps
        local_tables = {}
        local_seqs = {}
        for tbl in tables.values():
            if tbl['local']:
                local_tables[tbl['table_name']] = tbl['dest_table']
        for seq in seqs.values():
            if seq['local']:
                local_seqs[seq['seq_name']] = seq['seq_name']

        # set replica role for EXECUTE transaction
        if db.server_version >= 80300:
            curs.execute("set local session_replication_role = 'local'")

        for fn in files:
            fname = os.path.basename(fn)
            sql = open(fn, "r").read()
            attrs = ExecAttrs(sql = sql)
            q = "select * from londiste.execute_start(%s, %s, %s, true, %s)"
            res = self.exec_cmd(db, q, [self.queue_name, fname, sql, attrs.to_urlenc()], commit = False)
            ret = res[0]['ret_code']
            if ret >= 300:
                self.log.warning("Skipping execution of '%s'" % fname)
                continue
            if attrs.need_execute(curs, local_tables, local_seqs):
                self.log.info("%s: executing sql", fname)
                xsql = attrs.process_sql(sql, local_tables, local_seqs)
                for stmt in skytools.parse_statements(xsql):
                    curs.execute(stmt)
            else:
                self.log.info("%s: This SQL does not need to run on this node.", fname)
            q = "select * from londiste.execute_finish(%s, %s)"
            self.exec_cmd(db, q, [self.queue_name, fname], commit = False)
        db.commit()

    def get_provider_db(self):

        # use custom node for copy
        if self.options.copy_node:
            source_node = self.options.copy_node
            m = self.queue_info.get_member(source_node)
            if not m:
                raise skytools.UsageError("Cannot find node <%s>", source_node)
            if source_node == self.local_node:
                raise skytools.UsageError("Cannot use itself as provider")
            self.provider_location = m.location

        if not self.provider_location:
            db = self.get_database('db')
            q = 'select * from pgq_node.get_node_info(%s)'
            res = self.exec_cmd(db, q, [self.queue_name], quiet = True)
            self.provider_location = res[0]['provider_location']
        return self.get_database('provider_db', connstr = self.provider_location)


    def expand_arg_list(self, db, kind, existing, args, needs_tbl=True):
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


        allow_nonexist = not needs_tbl
        if existing:
            res = self.solve_globbing(args, lst_exists, map_exists, map_missing, allow_nonexist)
        else:
            res = self.solve_globbing(args, lst_missing, map_missing, map_exists, allow_nonexist)
        return res


    def solve_globbing(self, args, full_list, full_map, reverse_map, allow_nonexist):
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
                elif allow_nonexist:
                    res_list.append(a)
                    res_map[a] = 1
                elif self.options.force:
                    self.log.warning("%s not available, but --force is used" % a)
                    res_list.append(a)
                    res_map[a] = 1
                else:
                    self.log.warning("%s not available" % a)
                    err = 1
        if err:
            raise skytools.UsageError("Cannot proceed")
        return res_list

    def load_extra_status(self, curs, node):
        """Fetch extra info."""
        CascadeAdmin.load_extra_status(self, curs, node)
        curs.execute("select * from londiste.get_table_list(%s)", [self.queue_name])
        n_ok = n_half = n_ign = 0
        for tbl in curs.fetchall():
            if not tbl['local']:
                n_ign += 1
            elif tbl['merge_state'] == 'ok':
                n_ok += 1
            else:
                n_half += 1
        node.add_info_line('Tables: %d/%d/%d' % (n_ok, n_half, n_ign))

    def cmd_wait_sync(self):
        self.load_local_info()

        dst_db = self.get_database('db')
        self.wait_for_sync(dst_db)

    def wait_for_sync(self, dst_db):
        self.log.info("Waiting until all tables are in sync")
        q = "select table_name, merge_state, local"\
            " from londiste.get_table_list(%s) where local"
        dst_curs = dst_db.cursor()

        partial = {}
        done_pos = 1
        startup_info = 0
        while 1:
            dst_curs.execute(q, [self.queue_name])
            rows = dst_curs.fetchall()
            dst_db.commit()

            cur_count = 0
            done_list = []
            for row in rows:
                if not row['local']:
                    continue
                tbl = row['table_name']
                if row['merge_state'] != 'ok':
                    partial[tbl] = 0
                    cur_count += 1
                elif tbl in partial:
                    if partial[tbl] == 0:
                        partial[tbl] = 1
                        done_list.append(tbl)

            if not startup_info:
                self.log.info("%d table(s) to copy", len(partial))
                startup_info = 1

            for done in done_list:
                self.log.info("%s: finished (%d/%d)", done, done_pos, len(partial))
                done_pos += 1

            if cur_count == 0:
                break

            self.sleep(2)

        self.log.info("All done")

    def resurrect_dump_event(self, ev, stats, batch_info):
        """Collect per-table stats."""

        super(LondisteSetup, self).resurrect_dump_event(ev, stats, batch_info)

        ROLLBACK = 'can rollback'
        NO_ROLLBACK = 'cannot rollback'

        if ev.ev_type == 'TRUNCATE':
            if 'truncated_tables' not in stats:
                stats['truncated_tables'] = []
            tlist = stats['truncated_tables']
            tbl = ev.ev_extra1
            if tbl not in tlist:
                tlist.append(tbl)
        elif ev.ev_type[:2] in ('I:', 'U:', 'D:', 'I', 'U', 'D'):
            op = ev.ev_type[0]
            tbl = ev.ev_extra1
            bak = ev.ev_extra3
            tblkey = 'table: %s' % tbl
            if tblkey not in stats:
                stats[tblkey] = [0,0,0,ROLLBACK]
            tinfo = stats[tblkey]
            if op == 'I':
                tinfo[0] += 1
            elif op == 'U':
                tinfo[1] += 1
                if not bak:
                    tinfo[3] = NO_ROLLBACK
            elif op == 'D':
                tinfo[2] += 1
                if not bak and ev.ev_type == 'D':
                    tinfo[3] = NO_ROLLBACK
