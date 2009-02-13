#! /usr/bin/env python

import sys, time, skytools

from pgq.cascade.consumer import CascadedConsumer

class PlainCascadedConsumer(CascadedConsumer):
    def process_remote_event(self, src_curs, dst_curs, ev):
        ev.tag_done()

if __name__ == '__main__':
    script = PlainCascadedConsumer('nop_consumer', 'dst_db', sys.argv[1:])
    script.start()

