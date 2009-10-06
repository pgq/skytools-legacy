"""PgQ ticker.

It will also launch maintenance job.
"""

import time, threading
import skytools

from pgq.maint import MaintenanceJob

__all__ = ['SmallTicker']

class SmallTicker(skytools.DBScript):
    """Ticker that periodically calls pgq.ticker()."""
    tick_count = 0
    maint_thread = None

    def __init__(self, args):
        skytools.DBScript.__init__(self, 'pgqadm', args)

        self.ticker_log_time = 0
        self.ticker_log_delay = 5*60

    def reload(self):
        skytools.DBScript.reload(self)
        self.ticker_log_delay = self.cf.getfloat("ticker_log_delay", 5*60)

    def startup(self):
        if self.maint_thread:
            return

        # launch maint thread
        self.maint_thread = MaintenanceJob(self, [self.cf.filename])
        t = threading.Thread(name = 'maint_thread',
                             target = self.maint_thread.run)
        t.setDaemon(1)
        t.start()

    def work(self):
        db = self.get_database("db", autocommit = 1)
        cx = db.cursor()

        # run ticker
        cx.execute("select pgq.ticker()")
        self.tick_count += cx.fetchone()[0]

        cur_time = time.time()
        if cur_time > self.ticker_log_time + self.ticker_log_delay:
            self.ticker_log_time = cur_time
            self.stat_increase('ticks', self.tick_count)
            self.tick_count = 0

