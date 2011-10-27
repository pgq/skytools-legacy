#! /usr/bin/env python

"""Compares tables in replication set.

Currently just does count(1) on both sides.
"""

import sys, skytools

__all__ = ['Comparator']

from londiste.syncer import Syncer

class Comparator(Syncer):
    """Simple checker based in Syncer.
    When tables are in sync runs simple SQL query on them.
    """
    def process_sync(self, src_tbl, dst_tbl, src_db, dst_db):
        """Actual comparision."""

        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        self.log.info('Counting %s' % dst_tbl)

        q = "select count(1) as cnt, sum(hashtext(t.*::text)) as chksum from only _TABLE_ t"
        q = self.cf.get('compare_sql', q)
        src_q = q.replace('_TABLE_', skytools.quote_fqident(src_tbl))
        dst_q = q.replace('_TABLE_', skytools.quote_fqident(dst_tbl))

        f = "%(cnt)d rows, checksum=%(chksum)s"
        f = self.cf.get('compare_fmt', f)

        self.log.debug("srcdb: " + src_q)
        src_curs.execute(src_q)
        src_row = src_curs.fetchone()
        src_str = f % src_row
        self.log.info("srcdb: %s" % src_str)
        src_db.commit()

        self.log.debug("dstdb: " + dst_q)
        dst_curs.execute(dst_q)
        dst_row = dst_curs.fetchone()
        dst_str = f % dst_row
        self.log.info("dstdb: %s" % dst_str)
        dst_db.commit()

        if src_str != dst_str:
            self.log.warning("%s: Results do not match!" % dst_tbl)

if __name__ == '__main__':
    script = Comparator(sys.argv[1:])
    script.start()

