
"""Catch moment when tables are in sync on master and slave.
"""

import sys, time, skytools
from londiste.handler import build_handler, load_handler_modules

from londiste.util import find_copy_source

class ATable:
    def __init__(self, row):
        self.table_name = row['table_name']
        self.dest_table = row['dest_table'] or row['table_name']
        self.merge_state = row['merge_state']
        attrs = row['table_attrs'] or ''
        self.table_attrs = skytools.db_urldecode(attrs)
        hstr = self.table_attrs.get('handler', '')
        self.plugin = build_handler(self.table_name, hstr, row['dest_table'])

class Syncer(skytools.DBScript):
    """Walks tables in primary key order and checks if data matches."""

    bad_tables = 0

    provider_info = None

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

        load_handler_modules(self.cf)

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

    def get_provider_info(self, setup_curs):
        q = "select ret_code, ret_note, node_name, node_type, worker_name"\
            " from pgq_node.get_node_info(%s)"
        res = self.exec_cmd(setup_curs, q, [self.queue_name])
        pnode = res[0]
        self.log.info('Provider: %s (%s)', pnode['node_name'], pnode['node_type'])
        return pnode

    def check_consumer(self, setup_db, dst_db):
        """Before locking anything check if consumer is working ok."""

        setup_curs = setup_db.cursor()
        dst_curs = dst_db.cursor()
        c = 0
        while 1:
            q = "select * from pgq_node.get_consumer_state(%s, %s)"
            res = self.exec_cmd(dst_db, q, [self.queue_name, self.consumer_name])
            completed_tick = res[0]['completed_tick']

            q = "select extract(epoch from ticker_lag) from pgq.get_queue_info(%s)"
            setup_curs.execute(q, [self.queue_name])
            ticker_lag = setup_curs.fetchone()[0]

            q = "select extract(epoch from (now() - t.tick_time)) as lag"\
                " from pgq.tick t, pgq.queue q"\
                " where q.queue_name = %s"\
                "   and t.tick_queue = q.queue_id"\
                "   and t.tick_id = %s"
            setup_curs.execute(q, [self.queue_name, completed_tick])
            res = setup_curs.fetchall()

            if len(res) == 0:
                self.log.warning('Consumer completed_tick (%d) to not exists on provider (%s), too big lag?',
                                 completed_tick, self.provider_info['node_name'])
                self.sleep(10)
                continue

            consumer_lag = res[0][0]
            if consumer_lag < ticker_lag + 5:
                break

            lag_msg = 'Consumer lag: %s, ticker_lag %s, too big difference, waiting'
            if c % 30 == 0:
                self.log.warning(lag_msg, consumer_lag, ticker_lag)
            else:
                self.log.debug(lag_msg, consumer_lag, ticker_lag)
            c += 1
            time.sleep(1)

    def get_tables(self, db):
        """Load table info.

        Returns tuple of (dict(name->ATable), namelist)"""

        curs = db.cursor()
        q = "select table_name, merge_state, dest_table, table_attrs"\
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

        # 'SELECT 1' and COPY must use same snapshot, so change isolation level.
        dst_db = self.get_database('db', isolation_level = skytools.I_REPEATABLE_READ)
        pnode, ploc = self.get_provider_location(dst_db)

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
            t2 = dst_tables[tbl]
            if t2.merge_state != 'ok':
                self.log.warning('Table %s not synced yet, no point' % tbl)
                continue

            pnode, ploc, wname = find_copy_source(self, self.queue_name, tbl, pnode, ploc)
            self.log.info('%s: Using node %s as provider', tbl, pnode)

            if wname is None:
                wname = self.consumer_name
            self.downstream_worker_name = wname

            self.process_one_table(tbl, t2, dst_db, pnode, ploc)

        # signal caller about bad tables
        sys.exit(self.bad_tables)

    def process_one_table(self, tbl, t2, dst_db, provider_node, provider_loc):

        lock_db = self.get_database('lock_db', connstr = provider_loc)
        setup_db = self.get_database('setup_db', autocommit = 1, connstr = provider_loc)

        src_db = self.get_database('provider_db', connstr = provider_loc,
                                   isolation_level = skytools.I_REPEATABLE_READ)

        setup_curs = setup_db.cursor()

        # provider node info
        self.provider_info = self.get_provider_info(setup_curs)

        src_tables, ignore = self.get_tables(src_db)
        if not tbl in src_tables:
            self.log.warning('Table not available on provider: %s' % tbl)
            return
        t1 = src_tables[tbl]

        if t1.merge_state != 'ok':
            self.log.warning('Table %s not ready yet on provider' % tbl)
            return

        #self.check_consumer(setup_db, dst_db)

        self.check_table(t1, t2, lock_db, src_db, dst_db, setup_db)
        lock_db.commit()
        src_db.commit()
        dst_db.commit()

        self.close_database('setup_db')
        self.close_database('lock_db')
        self.close_database('provider_db')

    def force_tick(self, setup_curs, wait=True):
        q = "select pgq.force_tick(%s)"
        setup_curs.execute(q, [self.queue_name])
        res = setup_curs.fetchone()
        cur_pos = res[0]
        if not wait:
            return cur_pos

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
            #if dur > 10 and not self.options.force:
            #    raise Exception("Ticker seems dead")

    def check_table(self, t1, t2, lock_db, src_db, dst_db, setup_db):
        """Get transaction to same state, then process."""

        src_tbl = t1.dest_table
        dst_tbl = t2.dest_table

        lock_curs = lock_db.cursor()
        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        if not skytools.exists_table(src_curs, src_tbl):
            self.log.warning("Table %s does not exist on provider side" % src_tbl)
            return
        if not skytools.exists_table(dst_curs, dst_tbl):
            self.log.warning("Table %s does not exist on subscriber side" % dst_tbl)
            return

        # lock table against changes
        try:
            if self.provider_info['node_type'] == 'root':
                self.lock_table_root(lock_db, setup_db, dst_db, src_tbl, dst_tbl)
            else:
                self.lock_table_branch(lock_db, setup_db, dst_db, src_tbl, dst_tbl)

            # take snapshot on provider side
            src_db.commit()
            src_curs.execute("SELECT 1")

            # take snapshot on subscriber side
            dst_db.commit()
            dst_curs.execute("SELECT 1")
        finally:
            # release lock
            if self.provider_info['node_type'] == 'root':
                self.unlock_table_root(lock_db, setup_db)
            else:
                self.unlock_table_branch(lock_db, setup_db)

        # do work
        bad = self.process_sync(t1, t2, src_db, dst_db)
        if bad:
            self.bad_tables += 1

        # done
        src_db.commit()
        dst_db.commit()

    def lock_table_root(self, lock_db, setup_db, dst_db, src_tbl, dst_tbl):

        setup_curs = setup_db.cursor()
        lock_curs = lock_db.cursor()

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

        # now wait
        while 1:
            time.sleep(0.5)

            q = "select * from pgq_node.get_node_info(%s)"
            res = self.exec_cmd(dst_db, q, [self.queue_name])
            last_tick = res[0]['worker_last_tick']
            if last_tick > tick_id:
                break

            # limit lock time
            if time.time() > lock_time + self.lock_timeout and not self.options.force:
                self.log.error('Consumer lagging too much, exiting')
                lock_db.rollback()
                sys.exit(1)

    def unlock_table_root(self, lock_db, setup_db):
        lock_db.commit()

    def lock_table_branch(self, lock_db, setup_db, dst_db, src_tbl, dst_tbl):
        setup_curs = setup_db.cursor()

        lock_time = time.time()
        self.old_worker_paused = self.pause_consumer(setup_curs, self.provider_info['worker_name'])

        lock_curs = lock_db.cursor()
        self.log.info('Syncing %s' % dst_tbl)

        # consumer must get futher than this tick
        tick_id = self.force_tick(setup_curs, False)

        # now wait
        while 1:
            time.sleep(0.5)

            q = "select * from pgq_node.get_node_info(%s)"
            res = self.exec_cmd(dst_db, q, [self.queue_name])
            last_tick = res[0]['worker_last_tick']
            if last_tick > tick_id:
                break

            # limit lock time
            if time.time() > lock_time + self.lock_timeout and not self.options.force:
                self.log.error('Consumer lagging too much, exiting')
                lock_db.rollback()
                sys.exit(1)

    def unlock_table_branch(self, lock_db, setup_db):
        # keep worker paused if it was so before
        if self.old_worker_paused:
            return
        setup_curs = setup_db.cursor()
        self.resume_consumer(setup_curs, self.provider_info['worker_name'])

    def process_sync(self, t1, t2, src_db, dst_db):
        """It gets 2 connections in state where tbl should be in same state.
        """
        raise Exception('process_sync not implemented')

    def get_provider_location(self, dst_db):
        curs = dst_db.cursor()
        q = "select * from pgq_node.get_node_info(%s)"
        rows = self.exec_cmd(dst_db, q, [self.queue_name])
        return (rows[0]['provider_node'], rows[0]['provider_location'])

    def pause_consumer(self, curs, cons_name):
        self.log.info("Pausing upstream worker: %s", cons_name)
        return self.set_pause_flag(curs, cons_name, True)

    def resume_consumer(self, curs, cons_name):
        self.log.info("Resuming upstream worker: %s", cons_name)
        return self.set_pause_flag(curs, cons_name, False)

    def set_pause_flag(self, curs, cons_name, flag):
        q = "select * from pgq_node.get_consumer_state(%s, %s)"
        res = self.exec_cmd(curs, q, [self.queue_name, cons_name])
        oldflag = res[0]['paused']

        q = "select * from pgq_node.set_consumer_paused(%s, %s, %s)"
        self.exec_cmd(curs, q, [self.queue_name, cons_name, flag])

        while 1:
            q = "select * from pgq_node.get_consumer_state(%s, %s)"
            res = self.exec_cmd(curs, q, [self.queue_name, cons_name])
            if res[0]['uptodate']:
                break
            time.sleep(0.5)
        return oldflag

