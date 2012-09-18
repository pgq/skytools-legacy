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

        # get common cols
        cols = self.calc_cols(src_curs, src_tbl, dst_curs, dst_tbl)

        # get sane query
        v1 = src_db.server_version
        v2 = dst_db.server_version
        if (v1 < 80400 or v2 < 80400) and v1 != v2:
            q = "select count(1) as cnt, sum(md5('x'||substr(md5(_COLS_::text),1,16))::bit(64)::bigint) as chksum from only _TABLE_"
        else:
            q = "select count(1) as cnt, sum(hashtext(_COLS_::text)::bigint) as chksum from only _TABLE_"

        q = self.cf.get('compare_sql', q)
        q = q.replace("_COLS_", cols)
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
            return 1
        return 0

    def calc_cols(self, src_curs, src_tbl, dst_curs, dst_tbl):
        cols1 = self.load_cols(src_curs, src_tbl)
        cols2 = self.load_cols(dst_curs, dst_tbl)

        qcols = []
        for c in self.calc_common(cols1, cols2):
            qcols.append(skytools.quote_ident(c))
        return "(%s)" % ",".join(qcols)

    def load_cols(self, curs, tbl):
        schema, table = skytools.fq_name_parts(tbl)
        q = "select column_name from information_schema.columns"\
            " where table_schema = %s and table_name = %s"
        curs.execute(q, [schema, table])
        cols = []
        for row in curs.fetchall():
            cols.append(row[0])
        return cols

    def calc_common(self, cols1, cols2):
        common = []
        map2 = {}
        for c in cols2:
            map2[c] = 1
        for c in cols1:
            if c in map2:
                common.append(c)
        if len(common) == 0:
            raise Exception("no common columns found")

        if len(common) != len(cols1) or len(cols2) != len(cols1):
            self.log.warning("Ignoring some columns")

        return common

if __name__ == '__main__':
    script = Comparator(sys.argv[1:])
    script.start()

