
"""Writes events into file."""

import sys, os, skytools
from cStringIO import StringIO
from playback import *

__all__ = ['FileWrite']

class FileWrite(Replicator):
    """Writes events into file.
    
    Incomplete implementation.
    """

    last_successful_batch = None

    def load_state(self, batch_id):
        # maybe check if batch exists on filesystem?
        self.cur_tick = self.cur_batch_info['tick_id']
        self.prev_tick = self.cur_batch_info['prev_tick_id']
        return 1

    def process_batch(self, db, batch_id, ev_list):
        pass

    def save_state(self, do_commit):
        # nothing to save
        pass

    def sync_tables(self, dst_db):
        # nothing to sync
        return 1

    def interesting(self, ev):
        # wants all of them
        return 1

    def handle_data_event(self, ev):
        fmt = self.sql_command[ev.type]
        sql = fmt % (ev.ev_extra1, ev.data)
        row = "%s -- txid:%d" % (sql, ev.txid)
        self.sql_list.append(row)
        ev.tag_done()

    def handle_system_event(self, ev):
        row = "-- sysevent:%s txid:%d data:%s" % (
                ev.type, ev.txid, ev.data)
        self.sql_list.append(row)
        ev.tag_done()

    def flush_sql(self):
        self.sql_list.insert(0, "-- tick:%d prev:%s" % (
                             self.cur_tick, self.prev_tick))
        self.sql_list.append("-- end_tick:%d\n" % self.cur_tick)
        # store result
        dir = self.cf.get("file_dst")
        fn = os.path.join(dir, "tick_%010d.sql" % self.cur_tick)
        f = open(fn, "w")
        buf = "\n".join(self.sql_list)
        f.write(buf)
        f.close()

if __name__ == '__main__':
    script = Replicator(sys.argv[1:])
    script.start()

