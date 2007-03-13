
"""Reads events from file instead of db queue."""

import sys, os, re, skytools

from playback import *
from table_copy import *

__all__ = ['FileRead']

file_regex = r"^tick_0*([0-9]+)\.sql$"
file_rc = re.compile(file_regex)


class FileRead(CopyTable):
    """Reads events from file instead of db queue.
    
    Incomplete implementation.
    """

    def __init__(self, args, log = None):
        CopyTable.__init__(self, args, log, copy_thread = 0)

    def launch_copy(self, tbl):
        # copy immidiately
        self.do_copy(t)

    def work(self):
        last_batch = self.get_last_batch(curs)
        list = self.get_file_list()

    def get_list(self):
        """Return list of (first_batch, full_filename) pairs."""

        src_dir = self.cf.get('file_src')
        list = os.listdir(src_dir)
        list.sort()
        res = []
        for fn in list:
            m = file_rc.match(fn)
            if not m:
                self.log.debug("Ignoring file: %s" % fn)
                continue
            full = os.path.join(src_dir, fn)
            batch_id = int(m.group(1))
            res.append((batch_id, full))
        return res

if __name__ == '__main__':
    script = Replicator(sys.argv[1:])
    script.start()

