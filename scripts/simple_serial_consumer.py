#! /usr/bin/env python

"""simple serial consumer for skytools3

it consumes events from a predefined queue and feeds them to a sql statement

Config template::

[simple_serial_consumer]
job_name       = descriptive_name_for_job

src_db = dbname=sourcedb_test
dst_db = dbname=destdb port=1234 host=dbhost.com username=guest password=secret

pgq_queue_name = source_queue

logfile        = ~/log/%(job_name)s.log
pidfile        = ~/pid/%(job_name)s.pid

dst_query      = select 1

use_skylog     = 0
"""

"""Config example::

Create a queue named "echo_queue" in a database (like "testdb")

Register consumer "echo" to this queue

Start the echo consumer with config file shown below
(You may want to use -v to see, what will happen)

From some other window, insert something into the queue:
    select pgq.insert_event('echo_queue','type','hello=world');

Enjoy the ride :)

If dst_query is set to "select 1" then echo consumer becomes a sink consumer

[simple_serial_consumer]

job_name       = echo

src_db = dbname=testdb
dst_db = dbname=testdb

pgq_queue_name = echo_queue

logfile        = ~/log/%(job_name)s.log
pidfile        = ~/pid/%(job_name)s.pid

dst_query      =
        select *
        from pgq.insert_event('echo_queue', %%(pgq.ev_type)s, %%(pgq.ev_data)s)
"""

import sys, pgq, skytools
skytools.sane_config = 1

class SimpleSerialConsumer(pgq.SerialConsumer):
    def __init__(self, args):
        pgq.SerialConsumer.__init__(self,"simple_serial_consumer","src_db","dst_db", args)
        self.dst_query = self.cf.get("dst_query")

    def process_remote_batch(self, db, batch_id, event_list, dst_db):
        curs = dst_db.cursor()
        for ev in event_list:
            payload = skytools.db_urldecode(ev.data)
            if payload is None:
                payload = {}
            payload['pgq.ev_type'] = ev.type
            payload['pgq.ev_data'] = ev.data
            payload['pgq.ev_id'] = ev.id
            payload['pgq.ev_time'] = ev.time
            payload['pgq.ev_extra1'] = ev.extra1
            payload['pgq.ev_extra2'] = ev.extra2
            payload['pgq.ev_extra3'] = ev.extra3
            payload['pgq.ev_extra4'] = ev.extra4

            self.log.debug(self.dst_query % payload)
            curs.execute(self.dst_query, payload)
            try:
                res = curs.dictfetchone()
                self.log.debug(res)
            except:
                pass
            ev.tag_done()

if __name__ == '__main__':
    script = SimpleSerialConsumer(sys.argv[1:])
    script.start()
