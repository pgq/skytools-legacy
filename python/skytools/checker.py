#! /usr/bin/env python

"""Catch moment when tables are in sync on master and slave.
"""

import sys, time, os, subprocess

import pkgloader
pkgloader.require('skytools', '3.0')
import skytools

class TableRepair:
    """Checks that tables in two databases are in sync."""

    def __init__(self, table_name, log):
        self.table_name = table_name
        self.fq_table_name = skytools.quote_fqident(table_name)
        self.log = log
        self.reset()

    def reset(self):
        self.cnt_insert = 0
        self.cnt_update = 0
        self.cnt_delete = 0
        self.total_src = 0
        self.total_dst = 0
        self.pkey_list = []
        self.common_fields = []
        self.apply_fixes = False
        self.apply_cursor = None

    def do_repair(self, src_db, dst_db, where, pfx = 'repair', apply_fixes = False):
        """Actual comparision."""

        self.reset()

        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        self.apply_fixes = apply_fixes
        if apply_fixes:
            self.apply_cursor = dst_curs

        self.log.info('Checking %s' % self.table_name)

        copy_tbl = self.gen_copy_tbl(src_curs, dst_curs, where)

        dump_src = "%s.%s.src" % (pfx, self.table_name)
        dump_dst = "%s.%s.dst" % (pfx, self.table_name)
        fix = "%s.%s.fix" % (pfx, self.table_name)

        self.log.info("Dumping src table: %s" % self.table_name)
        self.dump_table(copy_tbl, src_curs, dump_src)
        src_db.commit()
        self.log.info("Dumping dst table: %s" % self.table_name)
        self.dump_table(copy_tbl, dst_curs, dump_dst)
        dst_db.commit()

        self.log.info("Sorting src table: %s" % self.table_name)
        self.do_sort(dump_src, dump_src + '.sorted')

        self.log.info("Sorting dst table: %s" % self.table_name)
        self.do_sort(dump_dst, dump_dst + '.sorted')

        self.dump_compare(dump_src + ".sorted", dump_dst + ".sorted", fix)

        os.unlink(dump_src)
        os.unlink(dump_dst)
        os.unlink(dump_src + ".sorted")
        os.unlink(dump_dst + ".sorted")

        if apply_fixes:
            dst_db.commit()

    def do_sort(self, src, dst):
        p = subprocess.Popen(["sort", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_ver = p.communicate()[0]
        del p

        xenv = os.environ.copy()
        xenv['LANG'] = 'C'
        xenv['LC_ALL'] = 'C'

        cmdline = ['sort', '-T', '.']
        if s_ver.find("coreutils") > 0:
            cmdline.append('-S')
            cmdline.append('30%')
        cmdline.append('-o')
        cmdline.append(dst)
        cmdline.append(src)
        p = subprocess.Popen(cmdline, env = xenv)
        if p.wait() != 0:
            raise Exception('sort failed')

    def gen_copy_tbl(self, src_curs, dst_curs, where):
        """Create COPY expession from common fields."""
        self.pkey_list = skytools.get_table_pkeys(src_curs, self.table_name)
        dst_pkey = skytools.get_table_pkeys(dst_curs, self.table_name)
        if dst_pkey != self.pkey_list:
            self.log.error('pkeys do not match')
            sys.exit(1)

        src_cols = skytools.get_table_columns(src_curs, self.table_name)
        dst_cols = skytools.get_table_columns(dst_curs, self.table_name)
        field_list = []
        for f in self.pkey_list:
            field_list.append(f)
        for f in src_cols:
            if f in self.pkey_list:
                continue
            if f in dst_cols:
                field_list.append(f)

        self.common_fields = field_list

        fqlist = [skytools.quote_ident(col) for col in field_list]

        tbl_expr = "select %s from %s" % (",".join(fqlist), self.fq_table_name)
        if where:
            tbl_expr += ' where ' + where
        tbl_expr = "COPY (%s) TO STDOUT" % tbl_expr

        self.log.debug("using copy expr: %s" % tbl_expr)

        return tbl_expr

    def dump_table(self, copy_cmd, curs, fn):
        """Dump table to disk."""
        f = open(fn, "w", 64*1024)
        curs.copy_expert(copy_cmd, f)
        self.log.info('%s: Got %d bytes' % (self.table_name, f.tell()))
        f.close()

    def get_row(self, ln):
        """Parse a row into dict."""
        if not ln:
            return None
        t = ln[:-1].split('\t')
        row = {}
        for i in range(len(self.common_fields)):
            row[self.common_fields[i]] = t[i]
        return row

    def dump_compare(self, src_fn, dst_fn, fix):
        """Dump + compare single table."""
        self.log.info("Comparing dumps: %s" % self.table_name)
        f1 = open(src_fn, "r", 64*1024)
        f2 = open(dst_fn, "r", 64*1024)
        src_ln = f1.readline()
        dst_ln = f2.readline()
        if src_ln: self.total_src += 1
        if dst_ln: self.total_dst += 1

        if os.path.isfile(fix):
            os.unlink(fix)

        while src_ln or dst_ln:
            keep_src = keep_dst = 0
            if src_ln != dst_ln:
                src_row = self.get_row(src_ln)
                dst_row = self.get_row(dst_ln)

                diff = self.cmp_keys(src_row, dst_row)
                if diff > 0:
                    # src > dst
                    self.got_missed_delete(dst_row, fix)
                    keep_src = 1
                elif diff < 0:
                    # src < dst
                    self.got_missed_insert(src_row, fix)
                    keep_dst = 1
                else:
                    if self.cmp_data(src_row, dst_row) != 0:
                        self.got_missed_update(src_row, dst_row, fix)

            if not keep_src:
                src_ln = f1.readline()
                if src_ln: self.total_src += 1
            if not keep_dst:
                dst_ln = f2.readline()
                if dst_ln: self.total_dst += 1

        self.log.info("finished %s: src: %d rows, dst: %d rows,"\
                    " missed: %d inserts, %d updates, %d deletes" % (
                self.table_name, self.total_src, self.total_dst,
                self.cnt_insert, self.cnt_update, self.cnt_delete))

    def got_missed_insert(self, src_row, fn):
        """Create sql for missed insert."""
        self.cnt_insert += 1
        fld_list = self.common_fields
        fq_list = []
        val_list = []
        for f in fld_list:
            fq_list.append(skytools.quote_ident(f))
            v = skytools.unescape_copy(src_row[f])
            val_list.append(skytools.quote_literal(v))
        q = "insert into %s (%s) values (%s);" % (
                self.fq_table_name, ", ".join(fq_list), ", ".join(val_list))
        self.show_fix(q, 'insert', fn)

    def got_missed_update(self, src_row, dst_row, fn):
        """Create sql for missed update."""
        self.cnt_update += 1
        fld_list = self.common_fields
        set_list = []
        whe_list = []
        for f in self.pkey_list:
            self.addcmp(whe_list, skytools.quote_ident(f), skytools.unescape_copy(src_row[f]))
        for f in fld_list:
            v1 = src_row[f]
            v2 = dst_row[f]
            if self.cmp_value(v1, v2) == 0:
                continue

            self.addeq(set_list, skytools.quote_ident(f), skytools.unescape_copy(v1))
            self.addcmp(whe_list, skytools.quote_ident(f), skytools.unescape_copy(v2))

        q = "update only %s set %s where %s;" % (
                self.fq_table_name, ", ".join(set_list), " and ".join(whe_list))
        self.show_fix(q, 'update', fn)

    def got_missed_delete(self, dst_row, fn):
        """Create sql for missed delete."""
        self.cnt_delete += 1
        whe_list = []
        for f in self.pkey_list:
            self.addcmp(whe_list, skytools.quote_ident(f), skytools.unescape_copy(dst_row[f]))
        q = "delete from only %s where %s;" % (self.fq_table_name, " and ".join(whe_list))
        self.show_fix(q, 'delete', fn)

    def show_fix(self, q, desc, fn):
        """Print/write/apply repair sql."""
        self.log.debug("missed %s: %s" % (desc, q))
        open(fn, "a").write("%s\n" % q)

        if self.apply_fixes:
            self.apply_cursor.execute(q)

    def addeq(self, list, f, v):
        """Add quoted SET."""
        vq = skytools.quote_literal(v)
        s = "%s = %s" % (f, vq)
        list.append(s)

    def addcmp(self, list, f, v):
        """Add quoted comparision."""
        if v is None:
            s = "%s is null" % f
        else:
            vq = skytools.quote_literal(v)
            s = "%s = %s" % (f, vq)
        list.append(s)

    def cmp_data(self, src_row, dst_row):
        """Compare data field-by-field."""
        for k in self.common_fields:
            v1 = src_row[k]
            v2 = dst_row[k]
            if self.cmp_value(v1, v2) != 0:
                return -1
        return 0

    def cmp_value(self, v1, v2):
        """Compare single field, tolerates tz vs notz dates."""
        if v1 == v2:
            return 0

        # try to work around tz vs. notz
        z1 = len(v1)
        z2 = len(v2)
        if z1 == z2 + 3 and z2 >= 19 and v1[z2] == '+':
            v1 = v1[:-3]
            if v1 == v2:
                return 0
        elif z1 + 3 == z2 and z1 >= 19 and v2[z1] == '+':
            v2 = v2[:-3]
            if v1 == v2:
                return 0

        return -1

    def cmp_keys(self, src_row, dst_row):
        """Compare primary keys of the rows.
        
        Returns 1 if src > dst, -1 if src < dst and 0 if src == dst"""

        # None means table is done.  tag it larger than any existing row.
        if src_row is None:
            if dst_row is None:
                return 0
            return 1
        elif dst_row is None:
            return -1

        for k in self.pkey_list:
            v1 = src_row[k]
            v2 = dst_row[k]
            if v1 < v2:
                return -1
            elif v1 > v2:
                return 1
        return 0


class Syncer(skytools.DBScript):
    """Checks that tables in two databases are in sync."""
    lock_timeout = 10
    ticker_lag_limit = 20
    consumer_lag_limit = 20

    def sync_table(self, cstr1, cstr2, queue_name, consumer_name, table_name):
        """Syncer main function.

        Returns (src_db, dst_db) that are in transaction
        where table should be in sync.
        """

        setup_db = self.get_database('setup_db', connstr = cstr1, autocommit = 1)
        lock_db = self.get_database('lock_db', connstr = cstr1)

        src_db = self.get_database('src_db', connstr = cstr1,
                isolation_level = skytools.I_REPEATABLE_READ)
        dst_db = self.get_database('dst_db', connstr = cstr2,
                isolation_level = skytools.I_REPEATABLE_READ)

        lock_curs = lock_db.cursor()
        setup_curs = setup_db.cursor()
        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        self.check_consumer(setup_curs, queue_name, consumer_name)

        # lock table in separate connection
        self.log.info('Locking %s' % table_name)
        self.set_lock_timeout(lock_curs)
        lock_time = time.time()
        lock_curs.execute("LOCK TABLE %s IN SHARE MODE" % skytools.quote_fqident(table_name))

        # now wait until consumer has updated target table until locking
        self.log.info('Syncing %s' % table_name)

        # consumer must get further than this tick
        tick_id = self.force_tick(setup_curs, queue_name)
        # try to force second tick also
        self.force_tick(setup_curs, queue_name)

        # take server time
        setup_curs.execute("select to_char(now(), 'YYYY-MM-DD HH24:MI:SS.MS')")
        tpos = setup_curs.fetchone()[0]
        # now wait
        while 1:
            time.sleep(0.5)

            q = "select now() - lag > timestamp %s, now(), lag from pgq.get_consumer_info(%s, %s)"
            setup_curs.execute(q, [tpos, queue_name, consumer_name])
            res = setup_curs.fetchall()
            if len(res) == 0:
                raise Exception('No such consumer: %s/%s' % (queue_name, consumer_name))
            row = res[0]
            self.log.debug("tpos=%s now=%s lag=%s ok=%s" % (tpos, row[1], row[2], row[0]))
            if row[0]:
                break

            # limit lock time
            if time.time() > lock_time + self.lock_timeout:
                self.log.error('Consumer lagging too much, exiting')
                lock_db.rollback()
                sys.exit(1)

        # take snapshot on provider side
        src_db.commit()
        src_curs.execute("SELECT 1")

        # take snapshot on subscriber side
        dst_db.commit()
        dst_curs.execute("SELECT 1")

        # release lock
        lock_db.commit()

        self.close_database('setup_db')
        self.close_database('lock_db')

        return (src_db, dst_db)

    def set_lock_timeout(self, curs):
        ms = int(1000 * self.lock_timeout)
        if ms > 0:
            q = "SET LOCAL statement_timeout = %d" % ms
            self.log.debug(q)
            curs.execute(q)

    def check_consumer(self, curs, queue_name, consumer_name):
        """ Before locking anything check if consumer is working ok.
        """
        self.log.info("Queue: %s Consumer: %s" % (queue_name, consumer_name))

        curs.execute('select current_database()')
        self.log.info('Actual db: %s' % curs.fetchone()[0])

        # get ticker lag
        q = "select extract(epoch from ticker_lag) from pgq.get_queue_info(%s);"
        curs.execute(q, [queue_name])
        ticker_lag = curs.fetchone()[0]
        self.log.info("Ticker lag: %s" % ticker_lag)
        # get consumer lag
        q = "select extract(epoch from lag) from pgq.get_consumer_info(%s, %s);"
        curs.execute(q, [queue_name, consumer_name])
        res = curs.fetchall()
        if len(res) == 0:
            self.log.error('check_consumer: No such consumer: %s/%s' % (queue_name, consumer_name))
            sys.exit(1)
        consumer_lag = res[0][0]

        # check that lag is acceptable
        self.log.info("Consumer lag: %s" % consumer_lag)
        if consumer_lag > ticker_lag + 10:
            self.log.error('Consumer lagging too much, cannot proceed')
            sys.exit(1)

    def force_tick(self, curs, queue_name):
        """ Force tick into source queue so that consumer can move on faster
        """
        q = "select pgq.force_tick(%s)"
        curs.execute(q, [queue_name])
        res = curs.fetchone()
        cur_pos = res[0]

        start = time.time()
        while 1:
            time.sleep(0.5)
            curs.execute(q, [queue_name])
            res = curs.fetchone()
            if res[0] != cur_pos:
                # new pos
                return res[0]

            # dont loop more than 10 secs
            dur = time.time() - start
            if dur > 10 and not self.options.force:
                raise Exception("Ticker seems dead")


class Checker(Syncer):
    """Checks that tables in two databases are in sync.
    
    Config options::

        ## data_checker ##
        confdb = dbname=confdb host=confdb.service

        extra_connstr = user=marko

        # one of: compare, repair, repair-apply, compare-repair-apply
        check_type = compare

        # random params used in queries
        cluster_name =
        instance_name =
        proxy_host =
        proxy_db =

        # list of tables to be compared
        table_list = foo, bar, baz

        where_expr = (hashtext(key_user_name) & %%(max_slot)s) in (%%(slots)s)

        # gets no args
        source_query =
         select h.hostname, d.db_name
           from dba.cluster c
                join dba.cluster_host ch on (ch.key_cluster = c.id_cluster)
                join conf.host h on (h.id_host = ch.key_host)
                join dba.database d on (d.key_host = ch.key_host)
          where c.db_name = '%(cluster_name)s'
            and c.instance_name = '%(instance_name)s'
            and d.mk_db_type = 'partition'
            and d.mk_db_status = 'active'
          order by d.db_name, h.hostname


        target_query =
            select db_name, hostname, slots, max_slot
              from dba.get_cross_targets(%%(hostname)s, %%(db_name)s, '%(proxy_host)s', '%(proxy_db)s')

        consumer_query =
            select q.queue_name, c.consumer_name
              from conf.host h
              join dba.database d on (d.key_host = h.id_host)
              join dba.pgq_queue q on (q.key_database = d.id_database)
              join dba.pgq_consumer c on (c.key_queue = q.id_queue)
             where h.hostname = %%(hostname)s
               and d.db_name = %%(db_name)s
               and q.queue_name like 'xm%%%%'
    """

    def __init__(self, args):
        """Checker init."""
        Syncer.__init__(self, 'data_checker', args)
        self.set_single_loop(1)
        self.log.info('Checker starting %s' % str(args))

        self.lock_timeout = self.cf.getfloat('lock_timeout', 10)

        self.table_list = self.cf.getlist('table_list')

    def work(self):
        """Syncer main function."""

        source_query = self.cf.get('source_query')
        target_query = self.cf.get('target_query')
        consumer_query = self.cf.get('consumer_query')
        where_expr = self.cf.get('where_expr')
        extra_connstr = self.cf.get('extra_connstr')

        check = self.cf.get('check_type', 'compare')

        confdb = self.get_database('confdb', autocommit=1)
        curs = confdb.cursor()

        curs.execute(source_query)
        for src_row in curs.fetchall():
            s_host = src_row['hostname']
            s_db = src_row['db_name']

            curs.execute(consumer_query, src_row)
            r = curs.fetchone()
            consumer_name = r['consumer_name']
            queue_name = r['queue_name']

            curs.execute(target_query, src_row)
            for dst_row in curs.fetchall():
                d_db = dst_row['db_name']
                d_host = dst_row['hostname']

                cstr1 = "dbname=%s host=%s %s" % (s_db, s_host, extra_connstr)
                cstr2 = "dbname=%s host=%s %s" % (d_db, d_host, extra_connstr)
                where = where_expr % dst_row

                self.log.info('Source: db=%s host=%s queue=%s consumer=%s' % (
                                  s_db, s_host, queue_name, consumer_name))
                self.log.info('Target: db=%s host=%s where=%s' % (d_db, d_host, where))

                for tbl in self.table_list:
                    src_db, dst_db = self.sync_table(cstr1, cstr2, queue_name, consumer_name, tbl)
                    if check == 'compare':
                        self.do_compare(tbl, src_db, dst_db, where)
                    elif check == 'repair':
                        r = TableRepair(tbl, self.log)
                        r.do_repair(src_db, dst_db, where, 'fix.' + tbl, False)
                    elif check == 'repair-apply':
                        r = TableRepair(tbl, self.log)
                        r.do_repair(src_db, dst_db, where, 'fix.' + tbl, True)
                    elif check == 'compare-repair-apply':
                        ok = self.do_compare(tbl, src_db, dst_db, where)
                        if not ok:
                            r = TableRepair(tbl, self.log)
                            r.do_repair(src_db, dst_db, where, 'fix.' + tbl, True)
                    else:
                        raise Exception('unknown check type')
                    self.reset()

    def do_compare(self, tbl, src_db, dst_db, where):
        """Actual comparision."""

        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        self.log.info('Counting %s' % tbl)

        q = "select count(1) as cnt, sum(hashtext(t.*::text)) as chksum from only _TABLE_ t where %s;" %  where
        q = self.cf.get('compare_sql', q)
        q = q.replace('_TABLE_', skytools.quote_fqident(tbl))

        f = "%(cnt)d rows, checksum=%(chksum)s"
        f = self.cf.get('compare_fmt', f)

        self.log.debug("srcdb: " + q)
        src_curs.execute(q)
        src_row = src_curs.fetchone()
        src_str = f % src_row
        self.log.info("srcdb: %s" % src_str)

        self.log.debug("dstdb: " + q)
        dst_curs.execute(q)
        dst_row = dst_curs.fetchone()
        dst_str = f % dst_row
        self.log.info("dstdb: %s" % dst_str)

        src_db.commit()
        dst_db.commit()

        if src_str != dst_str:
            self.log.warning("%s: Results do not match!" % tbl)
            return False
        else:
            self.log.info("%s: OK!" % tbl)
            return True


if __name__ == '__main__':
    script = Checker(sys.argv[1:])
    script.start()

