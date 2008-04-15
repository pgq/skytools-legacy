

import sys, time, skytools, pgq.consumer

class RawQueue:
    queue_name = None
    consumer_name = None
    batch_id = None
    cur_tick = None
    prev_tick = None
    def __init__(self, queue_name, consumer_name):
        self.queue_name = queue_name
        self.consumer_name = consumer_name
        self.bulk_insert_buf = []
        self.bulk_insert_size = 200
        self.bulk_insert_fields = ['ev_id', 'ev_time', 'ev_type', 'ev_data', 'ev_extra1', 'ev_extra2', 'ev_extra3', 'ev_extra4']

    def next_batch(self, curs):
        q = "select * from pgq.next_batch(%s, %s)"
        curs.execute(q, [self.queue_name, self.consumer_name])
        self.batch_id = curs.fetchone()[0]

        if not self.batch_id:
            return self.batch_id

        q = "select tick_id, prev_tick_id from pgq.get_batch_info(%s)"
        curs.execute(q, [self.batch_id])
        inf = curs.dictfetchone()
        self.cur_tick = inf['tick_id']
        self.prev_tick = inf['prev_tick_id']

        return self.batch_id

    def finish_batch(self, curs):
        q = "select * from pgq.finish_batch(%s)"
        curs.execute(q, [self.batch_id])

    def get_batch_events(self, curs):
        return pgq.consumer._BatchWalker(curs, self.batch_id, self.queue_name)

    def bulk_insert(self, curs, ev):
        row = map(ev.__getattr__, self.bulk_insert_fields)
        self.bulk_insert_buf.append(row)
        if len(self.bulk_insert_buf) >= self.bulk_insert_size:
            self.finish_bulk_insert(curs)

    def finish_bulk_insert(self, curs):
        pgq.bulk_insert_events(curs, self.bulk_insert_buf,
                               self.bulk_insert_fields, self.queue_name)
        self.bulk_insert_buf = []

