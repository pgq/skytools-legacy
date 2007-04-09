#! /usr/bin/env python

"""Londiste setup and sanity checker.

"""
import sys, os, skytools
from installer import *

__all__ = ['ProviderSetup', 'SubscriberSetup']

def find_column_types(curs, table):
    table_oid = skytools.get_table_oid(curs, table)
    if table_oid == None:
        return None

    key_sql = """
        SELECT k.attname FROM pg_index i, pg_attribute k
         WHERE i.indrelid = %d AND k.attrelid = i.indexrelid
           AND i.indisprimary AND k.attnum > 0 AND NOT k.attisdropped
        """ % table_oid

    # find columns
    q = """
        SELECT a.attname as name,
               CASE WHEN k.attname IS NOT NULL
                    THEN 'k' ELSE 'v' END AS type
          FROM pg_attribute a LEFT JOIN (%s) k ON (k.attname = a.attname)
         WHERE a.attrelid = %d AND a.attnum > 0 AND NOT a.attisdropped
         ORDER BY a.attnum
         """ % (key_sql, table_oid)
    curs.execute(q)
    rows = curs.dictfetchall()
    return rows

def make_type_string(col_rows):
    res = map(lambda x: x['type'], col_rows)
    return "".join(res)

class CommonSetup(skytools.DBScript):
    def __init__(self, args):
        skytools.DBScript.__init__(self, 'londiste', args)
        self.set_single_loop(1)
        self.pidfile = self.pidfile + ".setup"

        self.pgq_queue_name = self.cf.get("pgq_queue_name")
        self.consumer_id = self.cf.get("pgq_consumer_id", self.job_name)
        self.fake = self.cf.getint('fake', 0)

        if len(self.args) < 3:
            self.log.error("need subcommand")
            sys.exit(1)

    def run(self):
        self.admin()

    def fetch_provider_table_list(self, curs):
        q = """select table_name, trigger_name
                 from londiste.provider_get_table_list(%s)"""
        curs.execute(q, [self.pgq_queue_name])
        return curs.dictfetchall()

    def get_provider_table_list(self):
        src_db = self.get_database('provider_db')
        src_curs = src_db.cursor()
        list = self.fetch_provider_table_list(src_curs)
        src_db.commit()
        res = []
        for row in list:
            res.append(row['table_name'])
        return res

    def get_provider_seqs(self, curs):
        q = """SELECT * from londiste.provider_get_seq_list(%s)"""
        curs.execute(q, [self.pgq_queue_name])
        res = []
        for row in curs.fetchall():
            res.append(row[0])
        return res

    def get_all_seqs(self, curs):
        q = """SELECT n.nspname || '.'|| c.relname
                 from pg_class c, pg_namespace n
                where n.oid = c.relnamespace 
                  and c.relkind = 'S'
                order by 1"""
        curs.execute(q)
        res = []
        for row in curs.fetchall():
            res.append(row[0])
        return res

    def check_provider_queue(self):
        src_db = self.get_database('provider_db')
        src_curs = src_db.cursor()
        q = "select count(1) from pgq.get_queue_info(%s)"
        src_curs.execute(q, [self.pgq_queue_name])
        ok = src_curs.fetchone()[0]
        src_db.commit()
        
        if not ok:
            self.log.error('Event queue does not exist yet')
            sys.exit(1)

    def fetch_subscriber_tables(self, curs):
        q = "select * from londiste.subscriber_get_table_list(%s)"
        curs.execute(q, [self.pgq_queue_name])
        return curs.dictfetchall()

    def get_subscriber_table_list(self):
        dst_db = self.get_database('subscriber_db')
        dst_curs = dst_db.cursor()
        list = self.fetch_subscriber_tables(dst_curs)
        dst_db.commit()
        res = []
        for row in list:
            res.append(row['table_name'])
        return res

    def init_optparse(self, parser=None):
        p = skytools.DBScript.init_optparse(self, parser)
        p.add_option("--expect-sync", action="store_true", dest="expect_sync",
                    help = "no copy needed", default=False)
        p.add_option("--skip-truncate", action="store_true", dest="skip_truncate",
                    help = "dont delete old data", default=False)
        p.add_option("--force", action="store_true",
                    help="force", default=False)
        return p


#
# Provider commands
#

class ProviderSetup(CommonSetup):

    def admin(self):
        cmd = self.args[2]
        if cmd == "tables":
            self.provider_show_tables()
        elif cmd == "add":
            self.provider_add_tables(self.args[3:])
        elif cmd == "remove":
            self.provider_remove_tables(self.args[3:])
        elif cmd == "add-seq":
            for seq in self.args[3:]:
                self.provider_add_seq(seq)
            self.provider_notify_change()
        elif cmd == "remove-seq":
            for seq in self.args[3:]:
                self.provider_remove_seq(seq)
            self.provider_notify_change()
        elif cmd == "install":
            self.provider_install()
        elif cmd == "seqs":
            self.provider_list_seqs()
        else:
            self.log.error('bad subcommand')
            sys.exit(1)

    def provider_list_seqs(self):
        src_db = self.get_database('provider_db')
        src_curs = src_db.cursor()
        list = self.get_provider_seqs(src_curs)
        src_db.commit()

        for seq in list:
            print seq

    def provider_install(self):
        src_db = self.get_database('provider_db')
        src_curs = src_db.cursor()
        install_provider(src_curs, self.log)

        # create event queue
        q = "select pgq.create_queue(%s)"
        self.exec_provider(q, [self.pgq_queue_name])

    def provider_add_tables(self, table_list):
        self.check_provider_queue()

        cur_list = self.get_provider_table_list()
        for tbl in table_list:
            if tbl.find('.') < 0:
                tbl = "public." + tbl
            if tbl not in cur_list:
                self.log.info('Adding %s' % tbl)
                self.provider_add_table(tbl)
            else:
                self.log.info("Table %s already added" % tbl)
        self.provider_notify_change()

    def provider_remove_tables(self, table_list):
        self.check_provider_queue()

        cur_list = self.get_provider_table_list()
        for tbl in table_list:
            if tbl.find('.') < 0:
                tbl = "public." + tbl
            if tbl not in cur_list:
                self.log.info('%s already removed' % tbl)
            else:
                self.log.info("Removing %s" % tbl)
                self.provider_remove_table(tbl)
        self.provider_notify_change()

    def provider_add_table(self, tbl):
        q = "select londiste.provider_add_table(%s, %s)"
        self.exec_provider(q, [self.pgq_queue_name, tbl])

    def provider_remove_table(self, tbl):
        q = "select londiste.provider_remove_table(%s, %s)"
        self.exec_provider(q, [self.pgq_queue_name, tbl])

    def provider_show_tables(self):
        self.check_provider_queue()
        list = self.get_provider_table_list()
        for tbl in list:
            print tbl

    def provider_notify_change(self):
        q = "select londiste.provider_notify_change(%s)"
        self.exec_provider(q, [self.pgq_queue_name])

    def provider_add_seq(self, seq):
        seq = skytools.fq_name(seq)
        q = "select londiste.provider_add_seq(%s, %s)"
        self.exec_provider(q, [self.pgq_queue_name, seq])

    def provider_remove_seq(self, seq):
        seq = skytools.fq_name(seq)
        q = "select londiste.provider_remove_seq(%s, %s)"
        self.exec_provider(q, [self.pgq_queue_name, seq])

    def exec_provider(self, sql, args):
        src_db = self.get_database('provider_db')
        src_curs = src_db.cursor()

        src_curs.execute(sql, args)

        if self.fake:
            src_db.rollback()
        else:
            src_db.commit()

#
# Subscriber commands
#

class SubscriberSetup(CommonSetup):

    def admin(self):
        cmd = self.args[2]
        if cmd == "tables":
            self.subscriber_show_tables()
        elif cmd == "missing":
            self.subscriber_missing_tables()
        elif cmd == "add":
            self.subscriber_add_tables(self.args[3:])
        elif cmd == "remove":
            self.subscriber_remove_tables(self.args[3:])
        elif cmd == "resync":
            self.subscriber_resync_tables(self.args[3:])
        elif cmd == "register":
            self.subscriber_register()
        elif cmd == "unregister":
            self.subscriber_unregister()
        elif cmd == "install":
            self.subscriber_install()
        elif cmd == "check":
            self.check_tables(self.get_provider_table_list())
        elif cmd == "fkeys":
            self.collect_fkeys(self.get_provider_table_list())
        elif cmd == "seqs":
            self.subscriber_list_seqs()
        elif cmd == "add-seq":
            self.subscriber_add_seq(self.args[3:])
        elif cmd == "remove-seq":
            self.subscriber_remove_seq(self.args[3:])
        else:
            self.log.error('bad subcommand: ' + cmd)
            sys.exit(1)

    def collect_fkeys(self, table_list):
        dst_db = self.get_database('subscriber_db')
        dst_curs = dst_db.cursor()

        oid_list = []
        for tbl in table_list:
            try:
                oid = skytools.get_table_oid(dst_curs, tbl)
                if oid:
                    oid_list.append(str(oid))
            except:
                pass
        if len(oid_list) == 0:
            print "No tables"
            return
        oid_str = ",".join(oid_list)

        q = "SELECT n.nspname || '.' || t.relname as tbl, c.conname as con,"\
            "       pg_get_constraintdef(c.oid) as def"\
            "  FROM pg_constraint c, pg_class t, pg_namespace n"\
            " WHERE c.contype = 'f' and c.conrelid in (%s)"\
            "   AND t.oid = c.conrelid AND n.oid = t.relnamespace" % oid_str
        dst_curs.execute(q)
        res = dst_curs.dictfetchall()
        dst_db.commit()

        print "-- dropping"
        for row in res:
            q = "ALTER TABLE ONLY %(tbl)s DROP CONSTRAINT %(con)s;"
            print q % row
        print "-- creating"
        for row in res:
            q = "ALTER TABLE ONLY %(tbl)s ADD CONSTRAINT %(con)s %(def)s;"
            print q % row

    def check_tables(self, table_list):
        src_db = self.get_database('provider_db')
        src_curs = src_db.cursor()
        dst_db = self.get_database('subscriber_db')
        dst_curs = dst_db.cursor()

        failed = 0
        for tbl in table_list:
            self.log.info('Checking %s' % tbl)
            if not skytools.exists_table(src_curs, tbl):
                self.log.error('Table %s missing from provider side' % tbl)
                failed += 1
            elif not skytools.exists_table(dst_curs, tbl):
                self.log.error('Table %s missing from subscriber side' % tbl)
                failed += 1
            else:
                failed += self.check_table_columns(src_curs, dst_curs, tbl)
                failed += self.check_table_triggers(dst_curs, tbl)

        src_db.commit()
        dst_db.commit()

        return failed

    def check_table_triggers(self, dst_curs, tbl):
        oid = skytools.get_table_oid(dst_curs, tbl)
        if not oid:
            self.log.error('Table %s not found' % tbl)
            return 1
        q = "select count(1) from pg_trigger where tgrelid = %s"
        dst_curs.execute(q, [oid])
        got = dst_curs.fetchone()[0]
        if got:
            self.log.error('found trigger on table %s (%s)' % (tbl, str(oid)))
            return 1
        else:
            return 0

    def check_table_columns(self, src_curs, dst_curs, tbl):
        src_colrows = find_column_types(src_curs, tbl)
        dst_colrows = find_column_types(dst_curs, tbl)

        src_cols = make_type_string(src_colrows)
        dst_cols = make_type_string(dst_colrows)
        if src_cols.find('k') < 0:
            self.log.error('provider table %s has no primary key (%s)' % (
                             tbl, src_cols))
            return 1
        if dst_cols.find('k') < 0:
            self.log.error('subscriber table %s has no primary key (%s)' % (
                             tbl, dst_cols))
            return 1

        if src_cols != dst_cols:
            self.log.warning('table %s structure is not same (%s/%s)'\
                 ', trying to continue' % (tbl, src_cols, dst_cols))

        err = 0
        for row in src_colrows:
            found = 0
            for row2 in dst_colrows:
                if row2['name'] == row['name']:
                    found = 1
                    break
            if not found:
                err = 1
                self.log.error('%s: column %s on provider not on subscriber'
                                    % (tbl, row['name']))
            elif row['type'] != row2['type']:
                err = 1
                self.log.error('%s: pk different on column %s'
                                    % (tbl, row['name']))

        return err

    def subscriber_install(self):
        dst_db = self.get_database('subscriber_db')
        dst_curs = dst_db.cursor()

        install_subscriber(dst_curs, self.log)

        if self.fake:
            self.log.debug('rollback')
            dst_db.rollback()
        else:
            self.log.debug('commit')
            dst_db.commit()

    def subscriber_register(self):
        src_db = self.get_database('provider_db')
        src_curs = src_db.cursor()
        src_curs.execute("select pgq.register_consumer(%s, %s)",
            [self.pgq_queue_name, self.consumer_id])
        src_db.commit()

    def subscriber_unregister(self):
        q = "select londiste.subscriber_set_table_state(%s, %s, NULL, NULL)"

        dst_db = self.get_database('subscriber_db')
        dst_curs = dst_db.cursor()
        tbl_rows = self.fetch_subscriber_tables(dst_curs)
        for row in tbl_rows:
            dst_curs.execute(q, [self.pgq_queue_name, row['table_name']])
        dst_db.commit()

        src_db = self.get_database('provider_db')
        src_curs = src_db.cursor()
        src_curs.execute("select pgq.unregister_consumer(%s, %s)",
            [self.pgq_queue_name, self.consumer_id])
        src_db.commit()

    def subscriber_show_tables(self):
        list = self.get_subscriber_table_list()
        for tbl in list:
            print tbl

    def subscriber_missing_tables(self):
        provider_tables = self.get_provider_table_list()
        subscriber_tables = self.get_subscriber_table_list()
        for tbl in provider_tables:
            if tbl not in subscriber_tables:
                print tbl

    def subscriber_add_tables(self, table_list):
        provider_tables = self.get_provider_table_list()
        subscriber_tables = self.get_subscriber_table_list()

        err = 0
        for tbl in table_list:
            tbl = skytools.fq_name(tbl)
            if tbl not in provider_tables:
                err = 1
                self.log.error("Table %s not attached to queue" % tbl)
        if err:
            if self.options.force:
                self.log.warning('--force used, ignoring errors')
            else:
                sys.exit(1)

        err = self.check_tables(table_list)
        if err:
            if self.options.force:
                self.log.warning('--force used, ignoring errors')
            else:
                sys.exit(1)

        dst_db = self.get_database('subscriber_db')
        dst_curs = dst_db.cursor()
        for tbl in table_list:
            tbl = skytools.fq_name(tbl)
            if tbl in subscriber_tables:
                self.log.info("Table %s already added" % tbl)
            else:
                self.log.info("Adding %s" % tbl)
                self.subscriber_add_one_table(dst_curs, tbl)
            dst_db.commit()

    def subscriber_remove_tables(self, table_list):
        subscriber_tables = self.get_subscriber_table_list()
        for tbl in table_list:
            tbl = skytools.fq_name(tbl)
            if tbl in subscriber_tables:
                self.subscriber_remove_one_table(tbl)
            else:
                self.log.info("Table %s already removed" % tbl)

    def subscriber_resync_tables(self, table_list):
        dst_db = self.get_database('subscriber_db')
        dst_curs = dst_db.cursor()
        list = self.fetch_subscriber_tables(dst_curs)
        for tbl in table_list:
            tbl = skytools.fq_name(tbl)
            tbl_row = None
            for row in list:
                if row['table_name'] == tbl:
                    tbl_row = row
                    break
            if not tbl_row:
                self.log.warning("Table %s not found" % tbl)
            elif tbl_row['merge_state'] != 'ok':
                self.log.warning("Table %s is not in stable state" % tbl)
            else:
                self.log.info("Resyncing %s" % tbl)
                q = "select londiste.subscriber_set_table_state(%s, %s, NULL, NULL)"
                dst_curs.execute(q, [self.pgq_queue_name, tbl])
        dst_db.commit()

    def subscriber_add_one_table(self, dst_curs, tbl):
        q = "select londiste.subscriber_add_table(%s, %s)"

        dst_curs.execute(q, [self.pgq_queue_name, tbl])
        if self.options.expect_sync and self.options.skip_truncate:
            self.log.error("Too many options: --expect-sync and --skip-truncate")
            sys.exit(1)
        elif self.options.expect_sync:
            q = "select londiste.subscriber_set_table_state(%s, %s, null, 'ok')"
            dst_curs.execute(q, [self.pgq_queue_name, tbl])
        elif self.options.skip_truncate:
            q = "select londiste.subscriber_set_skip_truncate(%s, %s, true)"
            dst_curs.execute(q, [self.pgq_queue_name, tbl])

    def subscriber_remove_one_table(self, tbl):
        q = "select londiste.subscriber_remove_table(%s, %s)"

        dst_db = self.get_database('subscriber_db')
        dst_curs = dst_db.cursor()
        dst_curs.execute(q, [self.pgq_queue_name, tbl])
        dst_db.commit()

    def get_subscriber_seq_list(self):
        dst_db = self.get_database('subscriber_db')
        dst_curs = dst_db.cursor()
        q = "SELECT * from londiste.subscriber_get_seq_list(%s)"
        dst_curs.execute(q, [self.pgq_queue_name])
        list = dst_curs.fetchall()
        dst_db.commit()
        res = []
        for row in list:
            res.append(row[0])
        return res

    def subscriber_list_seqs(self):
        list = self.get_subscriber_seq_list()
        for seq in list:
            print seq

    def subscriber_add_seq(self, seq_list):
        src_db = self.get_database('provider_db')
        src_curs = src_db.cursor()
        dst_db = self.get_database('subscriber_db')
        dst_curs = dst_db.cursor()
        
        prov_list = self.get_provider_seqs(src_curs)
        src_db.commit()
        
        full_list = self.get_all_seqs(dst_curs)
        cur_list = self.get_subscriber_seq_list()

        for seq in seq_list:
            seq = skytools.fq_name(seq)
            if seq not in prov_list:
                self.log.error('Seq %s does not exist on provider side' % seq)
                continue
            if seq not in full_list:
                self.log.error('Seq %s does not exist on subscriber side' % seq)
                continue
            if seq in cur_list:
                self.log.info('Seq %s already subscribed' % seq)
                continue

            self.log.info('Adding sequence: %s' % seq)
            q = "select londiste.subscriber_add_seq(%s, %s)"
            dst_curs.execute(q, [self.pgq_queue_name, seq])

        dst_db.commit()

    def subscriber_remove_seq(self, seq_list):
        dst_db = self.get_database('subscriber_db')
        dst_curs = dst_db.cursor()
        cur_list = self.get_subscriber_seq_list()

        for seq in seq_list:
            seq = skytools.fq_name(seq)
            if seq not in cur_list:
                self.log.warning('Seq %s not subscribed')
            else:
                self.log.info('Removing sequence: %s' % seq)
                q = "select londiste.subscriber_remove_seq(%s, %s)"
                dst_curs.execute(q, [self.pgq_queue_name, seq])
        dst_db.commit()

