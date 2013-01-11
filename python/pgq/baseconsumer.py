
"""PgQ consumer framework for Python.

todo:
    - pgq.next_batch_details()
    - tag_done() by default

"""

import sys, time, skytools

from pgq.event import *

__all__ = ['BaseConsumer', 'BaseBatchWalker']


class BaseBatchWalker(object):
    """Lazy iterator over batch events.

    Events are loaded using cursor.  It will be given
    as ev_list to process_batch(). It allows:

     - one for loop over events
     - len() after that
    """

    _event_class = Event

    def __init__(self, curs, batch_id, queue_name, fetch_size = 300, consumer_filter = None):
        self.queue_name = queue_name
        self.fetch_size = fetch_size
        self.sql_cursor = "batch_walker"
        self.curs = curs
        self.length = 0
        self.batch_id = batch_id
        self.fetch_status = 0 # 0-not started, 1-in-progress, 2-done
        self.consumer_filter = consumer_filter

    def __iter__(self):
        if self.fetch_status:
            raise Exception("BatchWalker: double fetch? (%d)" % self.fetch_status)
        self.fetch_status = 1

        q = "select * from pgq.get_batch_cursor(%s, %s, %s, %s)"
        self.curs.execute(q, [self.batch_id, self.sql_cursor, self.fetch_size, self.consumer_filter])
        # this will return first batch of rows

        q = "fetch %d from %s" % (self.fetch_size, self.sql_cursor)
        while 1:
            rows = self.curs.dictfetchall()
            if not len(rows):
                break

            self.length += len(rows)
            for row in rows:
                ev = self._event_class(self, self.queue_name, row)
                yield ev

            # if less rows than requested, it was final block
            if len(rows) < self.fetch_size:
                break

            # request next block of rows
            self.curs.execute(q)

        self.curs.execute("close %s" % self.sql_cursor)

        self.fetch_status = 2

    def __len__(self):
        return self.length


class BaseConsumer(skytools.DBScript):
    """Consumer base class.
        Do not subclass directly (use pgq.Consumer or pgq.LocalConsumer instead)

    Config template::

        ## Parameters for pgq.Consumer ##

        # queue name to read from
        queue_name =

        # override consumer name
        #consumer_name = %(job_name)s

        # filter out only events for specific tables
        #table_filter = table1, table2

        # whether to use cursor to fetch events (0 disables)
        #pgq_lazy_fetch = 300

        # whether to read from source size in autocommmit mode
        # not compatible with pgq_lazy_fetch
        # the actual user script on top of pgq.Consumer must also support it
        #pgq_autocommit = 0

        # whether to wait for specified number of events,
        # before assigning a batch (0 disables)
        #pgq_batch_collect_events = 0

        # whether to wait specified amount of time,
        # before assigning a batch (postgres interval)
        #pgq_batch_collect_interval =

        # whether to stay behind queue top (postgres interval)
        #pgq_keep_lag =

        # in how many seconds to write keepalive stats for idle consumers
        # this stats is used for detecting that consumer is still running
        #keepalive_stats = 300
    """

    # by default, use cursor-based fetch
    default_lazy_fetch = 300

    # should reader connection be used in autocommit mode
    pgq_autocommit = 0

    # proper variables
    consumer_name = None
    queue_name = None

    # compat variables
    pgq_queue_name = None
    pgq_consumer_id = None

    pgq_lazy_fetch = None
    pgq_min_count = None
    pgq_min_interval = None
    pgq_min_lag = None

    batch_info = None

    consumer_filter = None

    keepalive_stats = None
    # statistics: time spent waiting for events
    idle_start = None

    _batch_walker_class = BaseBatchWalker

    def __init__(self, service_name, db_name, args):
        """Initialize new consumer.

        @param service_name: service_name for DBScript
        @param db_name: name of database for get_database()
        @param args: cmdline args for DBScript
        """

        skytools.DBScript.__init__(self, service_name, args)

        self.db_name = db_name

        # compat params
        self.consumer_name = self.cf.get("pgq_consumer_id", '')
        self.queue_name = self.cf.get("pgq_queue_name", '')

        # proper params
        if not self.consumer_name:
            self.consumer_name = self.cf.get("consumer_name", self.job_name)
        if not self.queue_name:
            self.queue_name = self.cf.get("queue_name")

        self.stat_batch_start = 0

        # compat vars
        self.pgq_queue_name = self.queue_name
        self.consumer_id = self.consumer_name

        # set default just once
        self.pgq_autocommit = self.cf.getint("pgq_autocommit", self.pgq_autocommit)
        if self.pgq_autocommit and self.pgq_lazy_fetch:
            raise skytools.UsageError("pgq_autocommit is not compatible with pgq_lazy_fetch")
        self.set_database_defaults(self.db_name, autocommit = self.pgq_autocommit)

        self.idle_start = time.time()

    def reload(self):
        skytools.DBScript.reload(self)

        self.pgq_lazy_fetch = self.cf.getint("pgq_lazy_fetch", self.default_lazy_fetch)

        # set following ones to None if not set
        self.pgq_min_count = self.cf.getint("pgq_batch_collect_events", 0) or None
        self.pgq_min_interval = self.cf.get("pgq_batch_collect_interval", '') or None
        self.pgq_min_lag = self.cf.get("pgq_keep_lag", '') or None

        # filter out specific tables only
        tfilt = []
        for t in self.cf.getlist('table_filter', ''):
            tfilt.append(skytools.quote_literal(skytools.fq_name(t)))
        if len(tfilt) > 0:
            expr = "ev_extra1 in (%s)" % ','.join(tfilt)
            self.consumer_filter = expr

        self.keepalive_stats = self.cf.getint("keepalive_stats", 300)

    def startup(self):
        """Handle commands here.  __init__ does not have error logging."""
        if self.options.register:
            self.register_consumer()
            sys.exit(0)
        if self.options.unregister:
            self.unregister_consumer()
            sys.exit(0)
        return skytools.DBScript.startup(self)

    def init_optparse(self, parser = None):
        p = skytools.DBScript.init_optparse(self, parser)
        p.add_option('--register', action='store_true',
                     help = 'register consumer on queue')
        p.add_option('--unregister', action='store_true',
                     help = 'unregister consumer from queue')
        return p

    def process_event(self, db, event):
        """Process one event.

        Should be overridden by user code.
        """
        raise Exception("needs to be implemented")

    def process_batch(self, db, batch_id, event_list):
        """Process all events in batch.

        By default calls process_event for each.
        Can be overridden by user code.
        """
        for ev in event_list:
            self.process_event(db, ev)

    def work(self):
        """Do the work loop, once (internal).
        Returns: true if wants to be called again,
        false if script can sleep.
        """

        db = self.get_database(self.db_name)
        curs = db.cursor()

        self.stat_start()

        # acquire batch
        batch_id = self._load_next_batch(curs)
        db.commit()
        if batch_id == None:
            return 0

        # load events
        ev_list = self._load_batch_events(curs, batch_id)
        db.commit()

        # process events
        self._launch_process_batch(db, batch_id, ev_list)

        # done
        self._finish_batch(curs, batch_id, ev_list)
        db.commit()
        self.stat_end(len(ev_list))

        return 1

    def register_consumer(self):
        self.log.info("Registering consumer on source queue")
        db = self.get_database(self.db_name)
        cx = db.cursor()
        cx.execute("select pgq.register_consumer(%s, %s)",
                [self.queue_name, self.consumer_name])
        res = cx.fetchone()[0]
        db.commit()

        return res

    def unregister_consumer(self):
        self.log.info("Unregistering consumer from source queue")
        db = self.get_database(self.db_name)
        cx = db.cursor()
        cx.execute("select pgq.unregister_consumer(%s, %s)",
                [self.queue_name, self.consumer_name])
        db.commit()

    def _launch_process_batch(self, db, batch_id, list):
        self.process_batch(db, batch_id, list)

    def _load_batch_events_old(self, curs, batch_id):
        """Fetch all events for this batch."""

        # load events
        sql = "select * from pgq.get_batch_events(%d)" % batch_id
        if self.consumer_filter is not None:
            sql += " where %s" % self.consumer_filter
        curs.execute(sql)
        rows = curs.dictfetchall()

        # map them to python objects
        ev_list = []
        for r in rows:
            ev = Event(self.queue_name, r)
            ev_list.append(ev)

        return ev_list

    def _load_batch_events(self, curs, batch_id):
        """Fetch all events for this batch."""

        if self.pgq_lazy_fetch:
            return self._batch_walker_class(curs, batch_id, self.queue_name, self.pgq_lazy_fetch, self.consumer_filter)
        else:
            return self._load_batch_events_old(curs, batch_id)

    def _load_next_batch(self, curs):
        """Allocate next batch. (internal)"""

        q = """select * from pgq.next_batch_custom(%s, %s, %s, %s, %s)"""
        curs.execute(q, [self.queue_name, self.consumer_name,
                         self.pgq_min_lag, self.pgq_min_count, self.pgq_min_interval])
        inf = curs.fetchone().copy()
        inf['tick_id'] = inf['cur_tick_id']
        inf['batch_end'] = inf['cur_tick_time']
        inf['batch_start'] = inf['prev_tick_time']
        inf['seq_start'] = inf['prev_tick_event_seq']
        inf['seq_end'] = inf['cur_tick_event_seq']
        self.batch_info = inf
        return self.batch_info['batch_id']

    def _finish_batch(self, curs, batch_id, list):
        """Tag events and notify that the batch is done."""

        curs.execute("select pgq.finish_batch(%s)", [batch_id])

    def stat_start(self):
        t = time.time()
        self.stat_batch_start = t
        if self.stat_batch_start - self.idle_start > self.keepalive_stats:
            self.stat_put('idle', round(self.stat_batch_start - self.idle_start,4))
            self.idle_start = t

    def stat_end(self, count):
        t = time.time()
        self.stat_put('count', count)
        self.stat_put('duration', round(t - self.stat_batch_start,4))
        if count > 0: # reset timer if we got some events
            self.stat_put('idle', round(self.stat_batch_start - self.idle_start,4))
            self.idle_start = t
