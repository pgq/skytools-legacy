
"""Catch moment when tables are in sync on master and slave.
"""

import sys, time, os

import pkgloader
pkgloader.require('skytools', '3.0')
import skytools

CONFDB = "dbname=confdb host=confdb.service user=replicator"

def unescape(s):
    """Remove copy escapes."""
    return skytools.unescape_copy(s)

def get_pkey_list(curs, tbl):
    """Get list of pkey fields in right order."""

    oid = skytools.get_table_oid(curs, tbl)
    q = """SELECT k.attname FROM pg_index i, pg_attribute k
            WHERE i.indrelid = %s AND k.attrelid = i.indexrelid
              AND i.indisprimary AND k.attnum > 0 AND NOT k.attisdropped
            ORDER BY k.attnum"""
    curs.execute(q, [oid])
    list = []
    for row in curs.fetchall():
        list.append(row[0])
    return list

def get_column_list(curs, tbl):
    """Get list of columns in right order."""

    oid = skytools.get_table_oid(curs, tbl)
    q = """SELECT a.attname FROM pg_attribute a
            WHERE a.attrelid = %s
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum"""
    curs.execute(q, [oid])
    list = []
    for row in curs.fetchall():
        list.append(row[0])
    return list

class Checker(skytools.DBScript):
    """Checks that tables in two databases are in sync."""
    cnt_insert = 0
    cnt_update = 0
    cnt_delete = 0
    total_src = 0
    total_dst = 0
    pkey_list = []
    common_fields = []

    def __init__(self, args):
        """Checker init."""
        skytools.DBScript.__init__(self, 'cross_mover', args)
        self.set_single_loop(1)
        self.log.info('Checker starting %s' % str(args))
	# compat names
        self.queue_name = self.cf.get("pgq_queue_name", '')
        self.consumer_name = self.cf.get('pgq_consumer_id', '')
        # good names
        if not self.queue_name:
            self.queue_name = self.cf.get("queue_name")
        if not self.consumer_name:
            self.consumer_name = self.cf.get('consumer_name', self.job_name)
        self.lock_timeout = self.cf.getfloat('lock_timeout', 10)
        # get tables to be compared
        if not self.options.table_list:
            self.log.error("--table is required")
        # create temp pidfile 
        if self.pidfile:
            self.pidfile += ".repair"

    def set_lock_timeout(self, curs):
        ms = int(1000 * self.lock_timeout)
        if ms > 0:
            q = "SET LOCAL statement_timeout = %d" % ms
            self.log.debug(q)
            curs.execute(q)

    def init_optparse(self, p=None):
        """ Initialize cmdline switches.
        """
        p = skytools.DBScript.init_optparse(self, p)
        p.add_option("--table", dest='table_list', help="space separated list of table names")
        p.add_option("--part_expr", dest='part_expr', help="table partitioning expression")

        return p

    def check_consumer(self, setup_curs):
        """ Before locking anything check if consumer is working ok.
        """
        self.log.info("Queue: %s Consumer: %s" % (self.queue_name, self.consumer_name)) 
        # get ticker lag
        q = "select extract(epoch from ticker_lag) from pgq.get_queue_info(%s);"
        setup_curs.execute(q, [self.queue_name])
        ticker_lag = setup_curs.fetchone()[0]
        self.log.info("Ticker lag: %s" % ticker_lag)
        # get consumer lag
        q = "select extract(epoch from lag) from pgq.get_consumer_info(%s, %s);"
        setup_curs.execute(q, [self.queue_name, self.consumer_name])
        res = setup_curs.fetchall()
        if len(res) == 0:
            self.log.error('No such consumer')
            sys.exit(1)
        consumer_lag = res[0][0]
        self.log.info("Consumer lag: %s" % consumer_lag) 
        # check that lag is acceptable
        if consumer_lag > ticker_lag + 10:
            self.log.error('Consumer lagging too much, cannot proceed')
            sys.exit(1)

    def work(self):
        """Syncer main function."""
        # get sourcedb connection and slots provided there
        setup_db = self.get_database('setup_db', autocommit = 1, connstr = self.cf.get('src_db'))
        setup_curs = setup_db.cursor()
        setup_curs.execute("select hostname(), current_database();")
        r_source = setup_curs.fetchone()
        self.log.info("Source: %s" % str(r_source))
        
        # get proxy db name and host (used to find out target cluster target partitons and their respective slots)
        proxy_db = self.get_database('dst_db', autocommit = 1)
        proxy_curs = proxy_db.cursor()
        proxy_curs.execute("select hostname(), current_database();")
        r_proxy = proxy_curs.fetchone()
        self.log.info("Proxy: %s" % str(r_proxy))
        
        # get target partitions from confdb and do also some sanity checks
        conf_db = self.get_database('conf_db', autocommit = 1, connstr = CONFDB)
        conf_curs = conf_db.cursor()
        q = "select db_name, hostname, slots, max_slot from dba.get_cross_targets(%s, %s, %s, %s)"
        conf_curs.execute(q, r_source + r_proxy)
        targets = conf_curs.fetchall()
        
        # get special purpose connections for magic locking
        lock_db = self.get_database('lock_db', connstr = self.cf.get('src_db'))
        src_db = self.get_database('src_db', isolation_level = skytools.I_SERIALIZABLE)
        
        # check that consumer is up and running 
        self.check_consumer(setup_curs)
        
        # loop over all tables and all targets
        mismatch_count = 0
        for tbl in self.options.table_list.split():
            self.log.info("Checking table: %s" % tbl)
            tbl = skytools.fq_name(tbl)
            for target in targets:
                self.log.info("Target: %s" % str(target))
                connstr = "dbname=%s host=%s user=replicator" % (target[0], target[1])
                fn = "%s.%s" % (target[1], target[0])
                dst_db = self.get_database(target[0], isolation_level = skytools.I_SERIALIZABLE, connstr = connstr)
                where = "%s & %s in (%s)" % (self.options.part_expr, target[3],target[2])
                if not self.check_table(tbl, lock_db, src_db, dst_db, setup_curs, where, fn):
                    mismatch_count += 1
                lock_db.commit()
                src_db.commit()
                dst_db.commit()
        if mismatch_count > 0:
            self.log.error("%s mismatching tables found" % mismatch_count)
            sys.exit(1)

    def force_tick(self, setup_curs):
        """ Force tick into source queue so that consumer can move on faster 
        """
        q = "select pgq.force_tick(%s)"
        setup_curs.execute(q, [self.queue_name])
        res = setup_curs.fetchone()
        cur_pos = res[0]

        start = time.time()
        while 1:
            time.sleep(0.5)
            setup_curs.execute(q, [self.queue_name])
            res = setup_curs.fetchone()
            if res[0] != cur_pos:
                # new pos
                return res[0]

            # dont loop more than 10 secs
            dur = time.time() - start
            if dur > 10 and not self.options.force:
                raise Exception("Ticker seems dead")

    def check_table(self, tbl, lock_db, src_db, dst_db, setup_curs, where, target):
        """ Get transaction to same state, then process.
        """
        lock_curs = lock_db.cursor()
        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        if not skytools.exists_table(src_curs, tbl):
            self.log.warning("Table %s does not exist on provider side" % tbl)
            return
        if not skytools.exists_table(dst_curs, tbl):
            self.log.warning("Table %s does not exist on subscriber side" % tbl)
            return

        # lock table in separate connection
        self.log.info('Locking %s' % tbl)
        lock_db.commit()
        self.set_lock_timeout(lock_curs)
        lock_time = time.time()
        lock_curs.execute("LOCK TABLE %s IN SHARE MODE" % skytools.quote_fqident(tbl))

        # now wait until consumer has updated target table until locking
        self.log.info('Syncing %s' % tbl)

        # consumer must get further than this tick
        tick_id = self.force_tick(setup_curs)
        # try to force second tick also
        self.force_tick(setup_curs)

        # take server time
        setup_curs.execute("select to_char(now(), 'YYYY-MM-DD HH24:MI:SS.MS')")
        tpos = setup_curs.fetchone()[0]
        # now wait
        while 1:
            time.sleep(0.5)

            q = "select now() - lag > timestamp %s, now(), lag from pgq.get_consumer_info(%s, %s)"
            setup_curs.execute(q, [tpos, self.queue_name, self.consumer_name])
            res = setup_curs.fetchall()
            if len(res) == 0:
                raise Exception('No such consumer')
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

        # do work
        result = self.do_compare(tbl, src_db, dst_db, where)
        if not result:
            self.do_repair(tbl, src_db, dst_db, where, target)
        # done
        src_db.commit()
        dst_db.commit()

        return result

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

        if src_str != dst_str:
            self.log.warning("%s: Results do not match!" % tbl)
            return False
        else:
            self.log.info("%s: OK!" % tbl)
            return True

    def do_repair(self, tbl, src_db, dst_db, where, target):
        """Actual comparision."""

        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        self.log.info('Checking %s' % tbl)

        self.common_fields = []
        self.pkey_list = []
        copy_tbl = self.gen_copy_tbl(tbl, src_curs, dst_curs, where)

        dump_src = target + "__" + tbl + ".src"
        dump_dst = target + "__" + tbl + ".dst"

        self.log.info("Dumping src table: %s" % tbl)
        self.dump_table(tbl, copy_tbl, src_curs, dump_src)
        src_db.commit()
        self.log.info("Dumping dst table: %s" % tbl)
        self.dump_table(tbl, copy_tbl, dst_curs, dump_dst)
        dst_db.commit()
        
        self.log.info("Sorting src table: %s" % tbl)

        s_in, s_out = os.popen4("sort --version")
        s_ver = s_out.read()
        del s_in, s_out
        if s_ver.find("coreutils") > 0:
            args = "-S 30%"
        else:
            args = ""
        os.system("sort %s -T . -o %s.sorted %s" % (args, dump_src, dump_src))
        self.log.info("Sorting dst table: %s" % tbl)
        os.system("sort %s -T . -o %s.sorted %s" % (args, dump_dst, dump_dst))

        self.dump_compare(tbl, dump_src + ".sorted", dump_dst + ".sorted", target)

        os.unlink(dump_src)
        os.unlink(dump_dst)
        os.unlink(dump_src + ".sorted")
        os.unlink(dump_dst + ".sorted")

    def gen_copy_tbl(self, tbl, src_curs, dst_curs, where):
        """Create COPY expession from common fields."""
        self.pkey_list = get_pkey_list(src_curs, tbl)
        dst_pkey = get_pkey_list(dst_curs, tbl)
        if dst_pkey != self.pkey_list:
            self.log.error('pkeys do not match')
            sys.exit(1)

        src_cols = get_column_list(src_curs, tbl)
        dst_cols = get_column_list(dst_curs, tbl)
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

        tbl_expr = "( select %s from %s where %s )" % (",".join(fqlist), skytools.quote_fqident(tbl), where)

        self.log.debug("using copy expr: %s" % tbl_expr)

        return tbl_expr

    def dump_table(self, tbl, copy_tbl, curs, fn):
        """Dump table to disk."""
        f = open(fn, "w", 64*1024)
        curs.copy_to(f, copy_tbl)
        size = f.tell()
        f.close()
        self.log.info('%s: Got %d bytes' % (tbl, size))

    def get_row(self, ln):
        """Parse a row into dict."""
        if not ln:
            return None
        t = ln[:-1].split('\t')
        row = {}
        for i in range(len(self.common_fields)):
            row[self.common_fields[i]] = t[i]
        return row

    def dump_compare(self, tbl, src_fn, dst_fn, target):
        """Dump + compare single table."""
        self.log.info("Comparing dumps: %s" % tbl)
        self.cnt_insert = 0
        self.cnt_update = 0
        self.cnt_delete = 0
        self.total_src = 0
        self.total_dst = 0
        f1 = open(src_fn, "r", 64*1024)
        f2 = open(dst_fn, "r", 64*1024)
        src_ln = f1.readline()
        dst_ln = f2.readline()
        if src_ln: self.total_src += 1
        if dst_ln: self.total_dst += 1

        fix = "fix.%s.%s.sql" % (target, tbl)
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
                    self.got_missed_delete(tbl, dst_row, fix)
                    keep_src = 1
                elif diff < 0:
                    # src < dst
                    self.got_missed_insert(tbl, src_row, fix)
                    keep_dst = 1
                else:
                    if self.cmp_data(src_row, dst_row) != 0:
                        self.got_missed_update(tbl, src_row, dst_row, fix)

            if not keep_src:
                src_ln = f1.readline()
                if src_ln: self.total_src += 1
            if not keep_dst:
                dst_ln = f2.readline()
                if dst_ln: self.total_dst += 1

        self.log.info("finished %s: src: %d rows, dst: %d rows,"\
                    " missed: %d inserts, %d updates, %d deletes" % (
                tbl, self.total_src, self.total_dst,
                self.cnt_insert, self.cnt_update, self.cnt_delete))

    def got_missed_insert(self, tbl, src_row, fn):
        """Create sql for missed insert."""
        self.cnt_insert += 1
        fld_list = self.common_fields
        fq_list = []
        val_list = []
        for f in fld_list:
            fq_list.append(skytools.quote_ident(f))
            v = unescape(src_row[f])
            val_list.append(skytools.quote_literal(v))
        q = "insert into %s (%s) values (%s);" % (
                tbl, ", ".join(fq_list), ", ".join(val_list))
        self.show_fix(tbl, q, 'insert', fn)

    def got_missed_update(self, tbl, src_row, dst_row, fn):
        """Create sql for missed update."""
        self.cnt_update += 1
        fld_list = self.common_fields
        set_list = []
        whe_list = []
        for f in self.pkey_list:
            self.addcmp(whe_list, skytools.quote_ident(f), unescape(src_row[f]))
        for f in fld_list:
            v1 = src_row[f]
            v2 = dst_row[f]
            if self.cmp_value(v1, v2) == 0:
                continue

            self.addeq(set_list, skytools.quote_ident(f), unescape(v1))
            self.addcmp(whe_list, skytools.quote_ident(f), unescape(v2))

        q = "update only %s set %s where %s;" % (
                tbl, ", ".join(set_list), " and ".join(whe_list))
        self.show_fix(tbl, q, 'update', fn)

    def got_missed_delete(self, tbl, dst_row, fn):
        """Create sql for missed delete."""
        self.cnt_delete += 1
        whe_list = []
        for f in self.pkey_list:
            self.addcmp(whe_list, skytools.quote_ident(f), unescape(dst_row[f]))
        q = "delete from only %s where %s;" % (skytools.quote_fqident(tbl), " and ".join(whe_list))
        self.show_fix(tbl, q, 'delete', fn)

    def show_fix(self, tbl, q, desc, fn):
        """Print/write/apply repair sql."""
        self.log.debug("missed %s: %s" % (desc, q))
        open(fn, "a").write("%s\n" % q)

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


if __name__ == '__main__':
    script = Checker(sys.argv[1:])
    script.start()

