#! /usr/bin/env python

import sys, time, skytools, pgq

class TestLocalConsumer(pgq.LocalConsumer):
    def process_local_event(self, src_db, batch_id, ev):
        self.log.info("event: type=%s data=%s", ev.type, ev.data)

if __name__ == '__main__':
    script = TestLocalConsumer('testconsumer', 'db', sys.argv[1:])
    script.start()

