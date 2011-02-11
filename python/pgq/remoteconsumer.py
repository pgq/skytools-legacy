
"""
old RemoteConsumer / SerialConsumer classes.

"""

import sys

from pgq.consumer import Consumer

__all__ = ['RemoteConsumer', 'SerialConsumer']

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
            return

        self.process_remote_batch(db, batch_id, event_list, dst_db)

        self.set_last_batch(curs, batch_id)
        dst_db.commit()

    def is_last_batch(self, dst_curs, batch_id):
        """Helper function to keep track of last successful batch
        in external database.
        """
        q = "select pgq_ext.is_batch_done(%s, %s)"
        dst_curs.execute(q, [ self.consumer_name, batch_id ])
        return dst_curs.fetchone()[0]

    def set_last_batch(self, dst_curs, batch_id):
        """Helper function to set last successful batch
        in external database.
        """
        q = "select pgq_ext.set_batch_done(%s, %s)"
        dst_curs.execute(q, [ self.consumer_name, batch_id ])

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
        self.dst_schema = "pgq_ext"

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

        # check if done
        if self.is_batch_done(curs):
            return

        # actual work
        self.process_remote_batch(db, batch_id, event_list, dst_db)

        # finish work
        self.set_batch_done(curs)
        dst_db.commit()

    def is_batch_done(self, dst_curs):
        """Helper function to keep track of last successful batch
        in external database.
        """

        cur_tick = self.batch_info['tick_id']
        prev_tick = self.batch_info['prev_tick_id']

        dst_tick = self.get_last_tick(dst_curs)
        if not dst_tick:
            # seems this consumer has not run yet against dst_db
            return False

        if prev_tick == dst_tick:
            # on track
            return False

        if cur_tick == dst_tick:
            # current batch is already applied, skip it
            return True

        # anything else means problems
        raise Exception('Lost position: batch %d..%d, dst has %d' % (
                        prev_tick, cur_tick, dst_tick))

    def set_batch_done(self, dst_curs):
        """Helper function to set last successful batch
        in external database.
        """
        tick_id = self.batch_info['tick_id']
        self.set_last_tick(dst_curs, tick_id)

    def register_consumer(self):
        new = Consumer.register_consumer(self)
        if new: # fixme
            self.dst_reset()

    def unregister_consumer(self):
        """If unregistering, also clean completed tick table on dest."""

        Consumer.unregister_consumer(self)
        self.dst_reset()

    def process_remote_batch(self, db, batch_id, event_list, dst_db):
        raise Exception('process_remote_batch not implemented')

    def rewind(self):
        self.log.info("Rewinding queue")
        src_db = self.get_database(self.db_name)
        dst_db = self.get_database(self.remote_db)
        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        dst_tick = self.get_last_tick(dst_curs)
        if dst_tick:
            q = "select pgq.register_consumer_at(%s, %s, %s)"
            src_curs.execute(q, [self.queue_name, self.consumer_name, dst_tick])
        else:
            self.log.warning('No tick found on dst side')

        dst_db.commit()
        src_db.commit()
        
    def dst_reset(self):
        self.log.info("Resetting queue tracking on dst side")
        dst_db = self.get_database(self.remote_db)
        dst_curs = dst_db.cursor()
        self.set_last_tick(dst_curs, None)
        dst_db.commit()
        
    def get_last_tick(self, dst_curs):
        q = "select %s.get_last_tick(%%s)" % self.dst_schema
        dst_curs.execute(q, [self.consumer_name])
        res = dst_curs.fetchone()
        return res[0]

    def set_last_tick(self, dst_curs, tick_id):
        q = "select %s.set_last_tick(%%s, %%s)" % self.dst_schema
        dst_curs.execute(q, [ self.consumer_name, tick_id ])


