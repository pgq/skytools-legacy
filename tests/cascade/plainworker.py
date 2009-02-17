#! /usr/bin/env python

import sys, time, skytools

from pgq.cascade.worker import CascadedWorker

class PlainCascadedWorker(CascadedWorker):
    def process_remote_event(self, src_curs, dst_curs, ev):
        self.log.info("got events: %s / %s" % (ev.ev_type, ev.ev_data))
        ev.tag_done()

if __name__ == '__main__':
    script = PlainCascadedWorker('nop_worker', 'dst_db', sys.argv[1:])
    script.start()

