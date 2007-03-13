
"""PgQ consumer framework for Python.

API problems(?):
    - process_event() and process_batch() should have db as argument.
    - should ev.tag*() update db immidiately?

"""

import sys, time, skytools

from pgq.event import *

__all__ = ['Consumer', 'RemoteConsumer', 'SerialConsumer']

class Consumer(skytools.DBScript):
    """Consumer base class.
    """

    def __init__(self, service_name, db_name, args):
        """Initialize new consumer.
        
        @param service_name: service_name for DBScript
        @param db_name: name of database for get_database()
        @param args: cmdline args for DBScript
        """

        skytools.DBScript.__init__(self, service_name, args)

        self.db_name = db_name
        self.reg_list = []
        self.consumer_id = self.cf.get("pgq_consumer_id", self.job_name)
        self.pgq_queue_name = self.cf.get("pgq_queue_name")

    def attach(self):
        """Attach consumer to interesting queues."""
        res = self.register_consumer(self.pgq_queue_name)
        return res

    def detach(self):
        """Detach consumer from all queues."""
        tmp = self.reg_list[:]
        for q in tmp:
            self.unregister_consumer(q)

    def process_event(self, db, event):
        """Process one event.

        Should be overrided by user code.

        Event should be tagged as done, retry or failed.
        If not, it will be tagged as for retry.
        """
        raise Exception("needs to be implemented")

    def process_batch(self, db, batch_id, event_list):
        """Process all events in batch.
        
        By default calls process_event for each.
        Can be overrided by user code.

        Events should be tagged as done, retry or failed.
        If not, they will be tagged as for retry.
        """
        for ev in event_list:
            self.process_event(db, ev)

    def work(self):
        """Do the work loop, once (internal)."""

        if len(self.reg_list) == 0:
            self.log.debug("Attaching")
            self.attach()

        db = self.get_database(self.db_name)
        curs = db.cursor()

        data_avail = 0
        for queue in self.reg_list:
            self.stat_start()

            # acquire batch
            batch_id = self._load_next_batch(curs, queue)
            db.commit()
            if batch_id == None:
                continue
            data_avail = 1

            # load events
            list = self._load_batch_events(curs, batch_id, queue)
            db.commit()
            
            # process events
            self._launch_process_batch(db, batch_id, list)

            # done
            self._finish_batch(curs, batch_id, list)
            db.commit()
            self.stat_end(len(list))

        # if false, script sleeps
        return data_avail

    def register_consumer(self, queue_name):
        db = self.get_database(self.db_name)
        cx = db.cursor()
        cx.execute("select pgq.register_consumer(%s, %s)",
                [queue_name, self.consumer_id])
        res = cx.fetchone()[0]
        db.commit()

        self.reg_list.append(queue_name)

        return res

    def unregister_consumer(self, queue_name):
        db = self.get_database(self.db_name)
        cx = db.cursor()
        cx.execute("select pgq.unregister_consumer(%s, %s)",
                    [queue_name, self.consumer_id])
        db.commit()

        self.reg_list.remove(queue_name)

    def _launch_process_batch(self, db, batch_id, list):
        self.process_batch(db, batch_id, list)

    def _load_batch_events(self, curs, batch_id, queue_name):
        """Fetch all events for this batch."""

        # load events
        sql = "select * from pgq.get_batch_events(%d)" % batch_id
        curs.execute(sql)
        rows = curs.dictfetchall()

        # map them to python objects
        list = []
        for r in rows:
            ev = Event(queue_name, r)
            list.append(ev)

        return list

    def _load_next_batch(self, curs, queue_name):
        """Allocate next batch. (internal)"""

        q = "select pgq.next_batch(%s, %s)"
        curs.execute(q, [queue_name, self.consumer_id])
        return curs.fetchone()[0]

    def _finish_batch(self, curs, batch_id, list):
        """Tag events and notify that the batch is done."""

        retry = failed = 0
        for ev in list:
            if ev.status == EV_FAILED:
                self._tag_failed(curs, batch_id, ev)
                failed += 1
            elif ev.status == EV_RETRY:
                self._tag_retry(curs, batch_id, ev)
                retry += 1
        curs.execute("select pgq.finish_batch(%s)", [batch_id])

    def _tag_failed(self, curs, batch_id, ev):
        """Tag event as failed. (internal)"""
        curs.execute("select pgq.event_failed(%s, %s, %s)",
                    [batch_id, ev.id, ev.fail_reason])

    def _tag_retry(self, cx, batch_id, ev):
        """Tag event for retry. (internal)"""
        cx.execute("select pgq.event_retry(%s, %s, %s)",
                    [batch_id, ev.id, ev.retry_time])

    def get_batch_info(self, batch_id):
        """Get info about batch.
        
        @return: Return value is a dict of:
        
          - queue_name: queue name
          - consumer_name: consumers name
          - batch_start: batch start time
          - batch_end: batch end time
          - tick_id: end tick id
          - prev_tick_id: start tick id
          - lag: how far is batch_end from current moment.
        """
        db = self.get_database(self.db_name)
        cx = db.cursor()
        q = "select queue_name, consumer_name, batch_start, batch_end,"\
            " prev_tick_id, tick_id, lag"\
            " from pgq.get_batch_info(%s)"
        cx.execute(q, [batch_id])
        row = cx.dictfetchone()
        db.commit()
        return row

    def stat_start(self):
        self.stat_batch_start = time.time()

    def stat_end(self, count):
        t = time.time()
        self.stat_add('count', count)
        self.stat_add('duration', t - self.stat_batch_start)


class RemoteConsumer(Consumer):
    """Helper for doing event processing in another database.

    Requires that whole batch is processed in one TX.
    """

    def __init__(self, service_name, db_name, remote_db, args):
        Consumer.__init__(self, service_name, db_name, args)
        self.remote_db = remote_db

    def process_batch(self, db, batch_id, event_list):
        """Process all events in batch.
        
        By default calls process_event for each.
        """
        dst_db = self.get_database(self.remote_db)
        curs = dst_db.cursor()

        if self.is_last_batch(curs, batch_id):
            for ev in event_list:
                ev.tag_done()
            return

        self.process_remote_batch(db, batch_id, event_list, dst_db)

        self.set_last_batch(curs, batch_id)
        dst_db.commit()

    def is_last_batch(self, dst_curs, batch_id):
        """Helper function to keep track of last successful batch
        in external database.
        """
        q = "select pgq_ext.is_batch_done(%s, %s)"
        dst_curs.execute(q, [ self.consumer_id, batch_id ])
        return dst_curs.fetchone()[0]

    def set_last_batch(self, dst_curs, batch_id):
        """Helper function to set last successful batch
        in external database.
        """
        q = "select pgq_ext.set_batch_done(%s, %s)"
        dst_curs.execute(q, [ self.consumer_id, batch_id ])

    def process_remote_batch(self, db, batch_id, event_list, dst_db):
        raise Exception('process_remote_batch not implemented')

class SerialConsumer(Consumer):
    """Consumer that applies batches sequentially in second database.

    Requirements:
     - Whole batch in one TX.
     - Must not use retry queue.

    Features:
     - Can detect if several batches are already applied to dest db.
     - If some ticks are lost. allows to seek back on queue.
       Whether it succeeds, depends on pgq configuration.
    """

    def __init__(self, service_name, db_name, remote_db, args):
        Consumer.__init__(self, service_name, db_name, args)
        self.remote_db = remote_db
        self.dst_completed_table = "pgq_ext.completed_tick"
        self.cur_batch_info = None

    def startup(self):
        if self.options.rewind:
            self.rewind()
            sys.exit(0)
        if self.options.reset:
            self.dst_reset()
            sys.exit(0)
        return Consumer.startup(self)

    def init_optparse(self, parser = None):
        p = Consumer.init_optparse(self, parser)
        p.add_option("--rewind", action = "store_true",
                help = "change queue position according to destination")
        p.add_option("--reset", action = "store_true",
                help = "reset queue pos on destination side")
        return p

    def process_batch(self, db, batch_id, event_list):
        """Process all events in batch.
        """

        dst_db = self.get_database(self.remote_db)
        curs = dst_db.cursor()

        self.cur_batch_info = self.get_batch_info(batch_id)

        # check if done
        if self.is_batch_done(curs):
            for ev in event_list:
                ev.tag_done()
            return

        # actual work
        self.process_remote_batch(db, batch_id, event_list, dst_db)

        # make sure no retry events
        for ev in event_list:
            if ev.status == EV_RETRY:
                raise Exception("SerialConsumer must not use retry queue")

        # finish work
        self.set_batch_done(curs)
        dst_db.commit()

    def is_batch_done(self, dst_curs):
        """Helper function to keep track of last successful batch
        in external database.
        """

        prev_tick = self.cur_batch_info['prev_tick_id']

        q = "select last_tick_id from %s where consumer_id = %%s" % (
                self.dst_completed_table ,)
        dst_curs.execute(q, [self.consumer_id])
        res = dst_curs.fetchone()

        if not res or not res[0]:
            # seems this consumer has not run yet against dst_db
            return False
        dst_tick = res[0]

        if prev_tick == dst_tick:
            # on track
            return False

        if prev_tick < dst_tick:
            self.log.warning('Got tick %d, dst has %d - skipping' % (prev_tick, dst_tick))
            return True
        else:
            self.log.error('Got tick %d, dst has %d - ticks lost' % (prev_tick, dst_tick))
            raise Exception('Lost ticks')

    def set_batch_done(self, dst_curs):
        """Helper function to set last successful batch
        in external database.
        """
        tick_id = self.cur_batch_info['tick_id']
        q = "delete from %s where consumer_id = %%s; "\
            "insert into %s (consumer_id, last_tick_id) values (%%s, %%s)" % (
                    self.dst_completed_table,
                    self.dst_completed_table)
        dst_curs.execute(q, [ self.consumer_id,
                              self.consumer_id, tick_id ])

    def attach(self):
        new = Consumer.attach(self)
        if new:
            self.clean_completed_tick()

    def detach(self):
        """If detaching, also clean completed tick table on dest."""

        Consumer.detach(self)
        self.clean_completed_tick()

    def clean_completed_tick(self):
        self.log.info("removing completed tick from dst")
        dst_db = self.get_database(self.remote_db)
        dst_curs = dst_db.cursor()

        q = "delete from %s where consumer_id = %%s" % (
                self.dst_completed_table,)
        dst_curs.execute(q, [self.consumer_id])
        dst_db.commit()

    def process_remote_batch(self, db, batch_id, event_list, dst_db):
        raise Exception('process_remote_batch not implemented')

    def rewind(self):
        self.log.info("Rewinding queue")
        src_db = self.get_database(self.db_name)
        dst_db = self.get_database(self.remote_db)
        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        q = "select last_tick_id from %s where consumer_id = %%s" % (
                self.dst_completed_table,)
        dst_curs.execute(q, [self.consumer_id])
        row = dst_curs.fetchone()
        if row:
            dst_tick = row[0]
            q = "select pgq.register_consumer(%s, %s, %s)"
            src_curs.execute(q, [self.pgq_queue_name, self.consumer_id, dst_tick])
        else:
            self.log.warning('No tick found on dst side')

        dst_db.commit()
        src_db.commit()
        
    def dst_reset(self):
        self.log.info("Resetting queue tracking on dst side")
        dst_db = self.get_database(self.remote_db)
        dst_curs = dst_db.cursor()

        q = "delete from %s where consumer_id = %%s" % (
                self.dst_completed_table,)
        dst_curs.execute(q, [self.consumer_id])
        dst_db.commit()
        

