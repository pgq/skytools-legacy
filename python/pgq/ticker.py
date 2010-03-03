"""PgQ ticker.

It will also launch maintenance job.
"""

import sys, os, time, threading
import skytools

from maint import MaintenanceJob

__all__ = ['SmartTicker']

def is_txid_sane(curs):
    curs.execute("select txid_current()")
    txid = curs.fetchone()[0]

    # on 8.2 theres no such table
    if not skytools.exists_table(curs, 'txid.epoch'):
        return 1

    curs.execute("select epoch, last_value from txid.epoch")
    epoch, last_val = curs.fetchone()
    stored_val = (epoch << 32) | last_val

    if stored_val <= txid:
        return 1
    else:
        return 0

class QueueStatus(object):
    def __init__(self, name):
        self.queue_name = name
        self.seq_name = None
        self.idle_period = 60
        self.max_lag = 3
        self.max_count = 200
        self.last_tick_time = 0
        self.last_count = 0
        self.quiet_count = 0

    def set_data(self, row):
        self.seq_name = row['queue_event_seq']
        self.idle_period = row['queue_ticker_idle_period']
        self.max_lag = row['queue_ticker_max_lag']
        self.max_count = row['queue_ticker_max_count']

    def need_tick(self, cur_count, cur_time):
        # check if tick is needed
        need_tick = 0
        lag = cur_time - self.last_tick_time

        if cur_count == self.last_count:
            # totally idle database

            # don't go immidiately to big delays, as seq grows before commit
            if self.quiet_count < 5:
                if lag >= self.max_lag:
                    need_tick = 1
                    self.quiet_count += 1
            else:
                if lag >= self.idle_period:
                    need_tick = 1
        else:
            self.quiet_count = 0
            # somewhat loaded machine
            if cur_count - self.last_count >= self.max_count:
                need_tick = 1
            elif lag >= self.max_lag:
                need_tick = 1
        if need_tick:
            self.last_tick_time = cur_time
            self.last_count = cur_count
        return need_tick

class SmartTicker(skytools.DBScript):
    last_tick_event = 0
    last_tick_time = 0
    quiet_count = 0
    tick_count = 0
    maint_thread = None

    def __init__(self, args):
        skytools.DBScript.__init__(self, 'pgqadm', args)

        self.ticker_log_time = 0
        self.ticker_log_delay = 5*60
        self.queue_map = {}
        self.refresh_time = 0

    def reload(self):
        skytools.DBScript.reload(self)
        self.ticker_log_delay = self.cf.getfloat("ticker_log_delay", 5*60)

    def startup(self):
        if self.maint_thread:
            return

        db = self.get_database("db", autocommit = 1)
        cx = db.cursor()
        ok = is_txid_sane(cx)
        if not ok:
            self.log.error('txid in bad state')
            sys.exit(1)

        self.maint_thread = MaintenanceJob(self, [self.cf.filename])
        t = threading.Thread(name = 'maint_thread',
                             target = self.maint_thread.run)
        t.setDaemon(1)
        t.start()

    def refresh_queues(self, cx):
        q = "select queue_name, queue_event_seq,"\
            " extract('epoch' from queue_ticker_idle_period) as queue_ticker_idle_period,"\
            " extract('epoch' from queue_ticker_max_lag) as queue_ticker_max_lag,"\
            " queue_ticker_max_count"\
            " from pgq.queue"\
            " where not queue_external_ticker"
        cx.execute(q)
        new_map = {}
        data_list = []
        for row in cx.dictfetchall():
            queue_name = row['queue_name']
            try:
                que = self.queue_map[queue_name]
            except KeyError, x:
                que = QueueStatus(queue_name)
            que.set_data(row)
            new_map[queue_name] = que

            p1 = "'%s', (select last_value from %s)" % (queue_name, que.seq_name)
            data_list.append(p1)

        self.queue_map = new_map
        self.seq_query = "select %s" % (
                ", ".join(data_list))

        if len(data_list) == 0:
            self.seq_query = None

        self.refresh_time = time.time()
        
    def work(self):
        db = self.get_database("db", autocommit = 1)
        cx = db.cursor()
        queue_refresh = self.cf.getint('queue_refresh_period', 30)

        cur_time = time.time()

        if cur_time >= self.refresh_time + queue_refresh:
            self.refresh_queues(cx)

        if not self.seq_query:
            return

        # now check seqs
        cx.execute(self.seq_query)
        res = cx.fetchone()
        pos = 0
        while pos < len(res):
            id = res[pos]
            val = res[pos + 1]
            pos += 2
            que = self.queue_map[id]
            if que.need_tick(val, cur_time):
                cx.execute("select pgq.ticker(%s)", [que.queue_name])
                self.tick_count += 1

        if cur_time > self.ticker_log_time + self.ticker_log_delay:
            self.ticker_log_time = cur_time
            self.stat_add('ticks', self.tick_count)
            self.tick_count = 0

