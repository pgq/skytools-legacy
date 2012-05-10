#!/usr/bin/python

"""Consumer that simply calls SQL query for each event.

Config::
    # query to call
    dst_query = select * from somefunc(%%(pgq.ev_data)s);

    # filter for events (SQL fragment)
    consumer_filter = ev_extra1 = 'public.mytable1'
"""


import sys

import pkgloader
pkgloader.require('skytools', '3.0')

import pgq
import skytools

class SimpleConsumer(pgq.Consumer):
    __doc__ = __doc__

    def __init__(self, args):
        pgq.Consumer.__init__(self,"simple_consumer3", "src_db", args)
        self.dst_query = self.cf.get("dst_query")
        self.consumer_filter = self.cf.get("consumer_filter", "")

    def process_event(self, db, ev):
        curs = self.get_database('dst_db', autocommit = 1).cursor()

        if ev.ev_type[:2] not in ('I:', 'U:', 'D:'):
            return

        if ev.ev_data is None:
            payload = {}
        else:
            payload = skytools.db_urldecode(ev.ev_data)
        payload['pgq.ev_data'] = ev.ev_data
        payload['pgq.ev_type'] = ev.ev_type
        payload['pgq.ev_extra1'] = ev.ev_extra1
        payload['pgq.ev_time'] = ev.ev_time
            
        self.log.debug(self.dst_query % payload)
        curs.execute(self.dst_query, payload)
        res = curs.fetchall()
        self.log.debug(res)

if __name__ == '__main__':
    script = SimpleConsumer(sys.argv[1:])
    script.start()

