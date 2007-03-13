
"""Catch moment when tables are in sync on master and slave.
"""

import sys, time, skytools

class Syncer(skytools.DBScript):
    """Walks tables in primary key order and checks if data matches."""

    def __init__(self, args):
        skytools.DBScript.__init__(self, 'londiste', args)
        self.set_single_loop(1)

        self.pgq_queue_name = self.cf.get("pgq_queue_name")
        self.pgq_consumer_id = self.cf.get('pgq_consumer_id', self.job_name)

        if self.pidfile:
            self.pidfile += ".repair"

    def init_optparse(self, p=None):
        p = skytools.DBScript.init_optparse(self, p)
        p.add_option("--force", action="store_true", help="ignore lag")
        return p

    def check_consumer(self, src_db):
        src_curs = src_db.cursor()
        
        # before locking anything check if consumer is working ok
        q = "select extract(epoch from ticker_lag) from pgq.get_queue_list()"\
                " where queue_name = %s"
        src_curs.execute(q, [self.pgq_queue_name])
        ticker_lag = src_curs.fetchone()[0]
        q = "select extract(epoch from lag)"\
            " from pgq.get_consumer_list()"\
            " where queue_name = %s"\
            "   and consumer_name = %s"
        src_curs.execute(q, [self.pgq_queue_name, self.pgq_consumer_id])
        res = src_curs.fetchall()
        src_db.commit()

        if len(res) == 0:
            self.log.error('No such consumer')
            sys.exit(1)
        consumer_lag = res[0][0]

        if consumer_lag > ticker_lag + 10 and not self.options.force:
            self.log.error('Consumer lagging too much, cannot proceed')
            sys.exit(1)

    def get_subscriber_table_state(self):
        dst_db = self.get_database('subscriber_db')
        dst_curs = dst_db.cursor()
        q = "select * from londiste.subscriber_get_table_list(%s)"
        dst_curs.execute(q, [self.pgq_queue_name])
        res = dst_curs.dictfetchall()
        dst_db.commit()
        return res

    def work(self):
        src_loc = self.cf.get('provider_db')
        lock_db = self.get_database('provider_db', cache='lock_db')
        src_db = self.get_database('provider_db')
        dst_db = self.get_database('subscriber_db')

        self.check_consumer(src_db)

        state_list = self.get_subscriber_table_state()
        state_map = {}
        full_list = []
        for ts in state_list:
            name = ts['table_name']
            full_list.append(name)
            state_map[name] = ts

        if len(self.args) > 2:
            tlist = self.args[2:]
        else:
            tlist = full_list

        for tbl in tlist:
            if not tbl in state_map:
                self.log.warning('Table not subscribed: %s' % tbl)
                continue
            st = state_map[tbl]
            if st['merge_state'] != 'ok':
                self.log.info('Table %s not synced yet, no point' % tbl)
                continue
            self.check_table(tbl, lock_db, src_db, dst_db)
            lock_db.commit()
            src_db.commit()
            dst_db.commit()

    def check_table(self, tbl, lock_db, src_db, dst_db):
        """Get transaction to same state, then process."""


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
        lock_curs.execute("LOCK TABLE %s IN SHARE MODE" % tbl)
        lock_time = time.time()

        # now wait until consumer has updated target table until locking
        self.log.info('Syncing %s' % tbl)

        # consumer must get futher than this tick
        src_curs.execute("select pgq.ticker(%s)", [self.pgq_queue_name])
        tick_id = src_curs.fetchone()[0]
        src_db.commit()
        # avoid depending on ticker by inserting second tick also
        time.sleep(0.1)
        src_curs.execute("select pgq.ticker(%s)", [self.pgq_queue_name])
        src_db.commit()
        src_curs.execute("select to_char(now(), 'YYYY-MM-DD HH24:MI:SS.MS')")
        tpos = src_curs.fetchone()[0]
        src_db.commit()
        # now wait
        while 1:
            time.sleep(0.2)

            q = """select now() - lag > %s, now(), lag
                     from pgq.get_consumer_list()
                   where consumer_name = %s
                     and queue_name = %s"""
            src_curs.execute(q, [tpos, self.pgq_consumer_id, self.pgq_queue_name])
            res = src_curs.fetchall()
            src_db.commit()

            if len(res) == 0:
                raise Exception('No such consumer')

            row = res[0]
            self.log.debug("tpos=%s now=%s lag=%s ok=%s" % (tpos, row[1], row[2], row[0]))
            if row[0]:
                break

            # loop max 10 secs
            if time.time() > lock_time + 10 and not self.options.force:
                self.log.error('Consumer lagging too much, exiting')
                lock_db.rollback()
                sys.exit(1)
        
        # take snapshot on provider side
        src_curs.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        src_curs.execute("SELECT 1")

        # take snapshot on subscriber side
        dst_db.commit()
        dst_curs.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        dst_curs.execute("SELECT 1")

        # release lock
        lock_db.commit()

        # do work
        self.process_sync(tbl, src_db, dst_db)

        # done
        src_db.commit()
        dst_db.commit()

    def process_sync(self, tbl, src_db, dst_db):
        """It gets 2 connections in state where tbl should be in same state.
        """
        raise Exception('process_sync not implemented')

