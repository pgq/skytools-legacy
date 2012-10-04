#! /usr/bin/env python

"""This script simply mover events from one queue to another.

Config parameters::

    ## Parameters for queue_mover

    src_db            = dbname=sourcedb
    dst_db            = dbname=targetdb

    dst_queue_name    = dest_queue
"""

import sys, os

import pkgloader
pkgloader.require('skytools', '3.0')

import pgq

class QueueMover(pgq.SerialConsumer):
    __doc__ = __doc__
    
    def __init__(self, args):
        pgq.SerialConsumer.__init__(self, "queue_mover3", "src_db", "dst_db", args)
        self.dst_queue_name = self.cf.get("dst_queue_name")

    def process_remote_batch(self, db, batch_id, ev_list, dst_db):

        # load data
        rows = []
        for ev in ev_list:
            data = [ev.type, ev.data, ev.extra1, ev.extra2, ev.extra3, ev.extra4, ev.time]
            rows.append(data)
        fields = ['type', 'data', 'extra1', 'extra2', 'extra3', 'extra4', 'time']

        # insert data
        curs = dst_db.cursor()
        pgq.bulk_insert_events(curs, rows, fields, self.dst_queue_name)

if __name__ == '__main__':
    script = QueueMover(sys.argv[1:])
    script.start()

