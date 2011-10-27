
"""Catch moment when tables are in sync on master and slave.
"""

import sys, time, skytools

class ATable:
    def __init__(self, row):
        self.table_name = row['table_name']
        self.dest_table = row['dest_table'] or row['table_name']
        self.merge_state = row['merge_state']

class Syncer(skytools.DBScript):
    """Walks tables in primary key order and checks if data matches."""

    def __init__(self, args):
        """Syncer init."""
        skytools.DBScript.__init__(self, 'londiste3', args)
        self.set_single_loop(1)

        # compat names
        self.queue_name = self.cf.get("pgq_queue_name", '')
        self.consumer_name = self.cf.get('pgq_consumer_id', '')

        # good names
        if not self.queue_name:
            self.queue_name = self.cf.get("queue_name")
        if not self.consumer_name:
            self.consumer_name = self.cf.get('consumer_name', self.job_name)

        self.lock_timeout = self.cf.getfloat('lock_timeout', 10)

        if self.pidfile:
            self.pidfile += ".repair"

    def set_lock_timeout(self, curs):
        ms = int(1000 * self.lock_timeout)
        if ms > 0:
            q = "SET LOCAL statement_timeout = %d" % ms
            self.log.debug(q)
            curs.execute(q)

    def init_optparse(self, p=None):
        """Initialize cmdline switches."""
        p = skytools.DBScript.init_optparse(self, p)
        p.add_option("--force", action="store_true", help="ignore lag")
        return p

    def check_consumer(self, setup_curs):
        """Before locking anything check if consumer is working ok."""

        q = "select extract(epoch from ticker_lag) from pgq.get_queue_info(%s)"
        setup_curs.execute(q, [self.queue_name])
        ticker_lag = setup_curs.fetchone()[0]
        q = "select extract(epoch from lag)"\
            " from pgq.get_consumer_info(%s, %s)"
        setup_curs.execute(q, [self.queue_name, self.consumer_name])
        res = setup_curs.fetchall()

        if len(res) == 0:
            self.log.error('No such consumer')
            sys.exit(1)
        consumer_lag = res[0][0]

        if consumer_lag > ticker_lag + 10 and not self.options.force:
            self.log.error('Consumer lagging too much, cannot proceed')
            sys.exit(1)

    def get_tables(self, db):
        """Load table info.

        Returns tuple of (dict(name->ATable), namelist)"""

        curs = db.cursor()
        q = "select table_name, merge_state, dest_table"\
            " from londiste.get_table_list(%s) where local"
        curs.execute(q, [self.queue_name])
        rows = curs.fetchall()
        db.commit()

        res = {}
        names = []
        for row in rows:
            t = ATable(row)
            res[t.table_name] = t
            names.append(t.table_name)
        return res, names

    def work(self):
        """Syncer main function."""
        dst_db = self.get_database('db', isolation_level = skytools.I_SERIALIZABLE)
        provider_loc = self.get_provider_location(dst_db)

        lock_db = self.get_database('lock_db', connstr = provider_loc)
        setup_db = self.get_database('setup_db', autocommit = 1, connstr = provider_loc)

        src_db = self.get_database('provider_db', connstr = provider_loc,
                                   isolation_level = skytools.I_SERIALIZABLE)

        setup_curs = setup_db.cursor()

        self.check_consumer(setup_curs)

        src_tables, ignore = self.get_tables(src_db)
        dst_tables, names = self.get_tables(dst_db)

        if len(self.args) > 2:
            tlist = self.args[2:]
        else:
            tlist = names

        for tbl in tlist:
            tbl = skytools.fq_name(tbl)
            if not tbl in dst_tables:
                self.log.warning('Table not subscribed: %s' % tbl)
                continue
            if not tbl in src_tables:
                self.log.warning('Table not available on provider: %s' % tbl)
                continue
            t1 = src_tables[tbl]
            t2 = dst_tables[tbl]

            if t1.merge_state != 'ok':
                self.log.warning('Table %s not ready yet on provider' % tbl)
                continue
            if t2.merge_state != 'ok':
                self.log.warning('Table %s not synced yet, no point' % tbl)
                continue
            self.check_table(t1.dest_table, t2.dest_table, lock_db, src_db, dst_db, setup_curs)
            lock_db.commit()
            src_db.commit()
            dst_db.commit()

    def force_tick(self, setup_curs):
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

    def check_table(self, src_tbl, dst_tbl, lock_db, src_db, dst_db, setup_curs):
        """Get transaction to same state, then process."""


        lock_curs = lock_db.cursor()
        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        if not skytools.exists_table(src_curs, src_tbl):
            self.log.warning("Table %s does not exist on provider side" % src_tbl)
            return
        if not skytools.exists_table(dst_curs, dst_tbl):
            self.log.warning("Table %s does not exist on subscriber side" % dst_tbl)
            return

        # lock table in separate connection
        self.log.info('Locking %s' % src_tbl)
        lock_db.commit()
        self.set_lock_timeout(lock_curs)
        lock_time = time.time()
        lock_curs.execute("LOCK TABLE %s IN SHARE MODE" % skytools.quote_fqident(src_tbl))

        # now wait until consumer has updated target table until locking
        self.log.info('Syncing %s' % dst_tbl)

        # consumer must get futher than this tick
        tick_id = self.force_tick(setup_curs)
        # try to force second tick also
        self.force_tick(setup_curs)

        # take server time
        setup_curs.execute("select to_char(now(), 'YYYY-MM-DD HH24:MI:SS.MS')")
        tpos = setup_curs.fetchone()[0]
        # now wait
        while 1:
            time.sleep(0.5)

            q = "select now() - lag > timestamp %s, now(), lag"\
                " from pgq.get_consumer_info(%s, %s)"
            setup_curs.execute(q, [tpos, self.queue_name, self.consumer_name])
            res = setup_curs.fetchall()

            if len(res) == 0:
                raise Exception('No such consumer')

            row = res[0]
            self.log.debug("tpos=%s now=%s lag=%s ok=%s" % (tpos, row[1], row[2], row[0]))
            if row[0]:
                break

            # limit lock time
            if time.time() > lock_time + self.lock_timeout and not self.options.force:
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
        self.process_sync(src_tbl, dst_tbl, src_db, dst_db)

        # done
        src_db.commit()
        dst_db.commit()

    def process_sync(self, src_tbl, dst_tbl, src_db, dst_db):
        """It gets 2 connections in state where tbl should be in same state.
        """
        raise Exception('process_sync not implemented')

    def get_provider_location(self, dst_db):
        curs = dst_db.cursor()
        q = "select * from pgq_node.get_node_info(%s)"
        rows = self.exec_cmd(dst_db, q, [self.queue_name])
        return rows[0]['provider_location']

