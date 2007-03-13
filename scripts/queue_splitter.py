#! /usr/bin/env python

# puts events into queue specified by field from 'queue_field' config parameter

import sys, os, pgq, skytools

class QueueSplitter(pgq.SerialConsumer):
    def __init__(self, args):
        pgq.SerialConsumer.__init__(self, "queue_splitter", "src_db", "dst_db", args)

    def process_remote_batch(self, db, batch_id, ev_list, dst_db):
        cache = {}
        queue_field = self.cf.get('queue_field', 'extra1')
        for ev in ev_list:
            row = [ev.type, ev.data, ev.extra1, ev.extra2, ev.extra3, ev.extra4, ev.time]
            queue = ev.__getattr__(queue_field)
            if queue not in cache:
                cache[queue] = []
            cache[queue].append(row)
            ev.tag_done()

        # should match the composed row
        fields = ['type', 'data', 'extra1', 'extra2', 'extra3', 'extra4', 'time']

        # now send them to right queues
        curs = dst_db.cursor()
        for queue, rows in cache.items():
            pgq.bulk_insert_events(curs, rows, fields, queue)

if __name__ == '__main__':
    script = QueueSplitter(sys.argv[1:])
    script.start()

