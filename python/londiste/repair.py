
"""Repair data on subscriber.

Walks tables by primary key and searcher
missing inserts/updates/deletes.
"""

import sys, os, time, skytools

try:
    import subprocess
    have_subprocess = True
except ImportError:
    have_subprocess = False

from syncer import Syncer

__all__ = ['Repairer']

def unescape(s):
    return skytools.unescape_copy(s)

def get_pkey_list(curs, tbl):
    """Get list of pkey fields in right order."""

    oid = skytools.get_table_oid(curs, tbl)
    q = """SELECT k.attname FROM pg_index i, pg_attribute k
            WHERE i.indrelid = %s AND k.attrelid = i.indexrelid
              AND i.indisprimary AND k.attnum > 0 AND NOT k.attisdropped
            ORDER BY k.attnum"""
    curs.execute(q, [oid])
    list = []
    for row in curs.fetchall():
        list.append(row[0])
    return list

def get_column_list(curs, tbl):
    """Get list of columns in right order."""

    oid = skytools.get_table_oid(curs, tbl)
    q = """SELECT a.attname FROM pg_attribute a
            WHERE a.attrelid = %s
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum"""
    curs.execute(q, [oid])
    list = []
    for row in curs.fetchall():
        list.append(row[0])
    return list

class Repairer(Syncer):
    """Walks tables in primary key order and checks if data matches."""


    def process_sync(self, tbl, src_db, dst_db):
        """Actual comparision."""

        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        self.log.info('Checking %s' % tbl)

        self.common_fields = []
        self.pkey_list = []
        copy_tbl = self.gen_copy_tbl(tbl, src_curs, dst_curs)

        dump_src = tbl + ".src"
        dump_dst = tbl + ".dst"

        self.log.info("Dumping src table: %s" % tbl)
        self.dump_table(tbl, copy_tbl, src_curs, dump_src)
        src_db.commit()
        self.log.info("Dumping dst table: %s" % tbl)
        self.dump_table(tbl, copy_tbl, dst_curs, dump_dst)
        dst_db.commit()
        
        self.log.info("Sorting src table: %s" % tbl)

        # check if sort supports -S
        if have_subprocess:
            p = subprocess.Popen(["sort", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            s_ver = p.communicate()[0]
            del p
        else:
            s_ver = os.popen4("sort --version")[1].read()
        if s_ver.find("coreutils") > 0:
            args = "-S 30%"
        else:
            args = ""

        os.system("LC_ALL=C sort %s -T . -o %s.sorted %s" % (args, dump_src, dump_src))
        self.log.info("Sorting dst table: %s" % tbl)
        os.system("LC_ALL=C sort %s -T . -o %s.sorted %s" % (args, dump_dst, dump_dst))

        self.dump_compare(tbl, dump_src + ".sorted", dump_dst + ".sorted")

        os.unlink(dump_src)
        os.unlink(dump_dst)
        os.unlink(dump_src + ".sorted")
        os.unlink(dump_dst + ".sorted")

    def gen_copy_tbl(self, tbl, src_curs, dst_curs):
        self.pkey_list = get_pkey_list(src_curs, tbl)
        dst_pkey = get_pkey_list(dst_curs, tbl)
        if dst_pkey != self.pkey_list:
            self.log.error('pkeys do not match')
            sys.exit(1)

        src_cols = get_column_list(src_curs, tbl)
        dst_cols = get_column_list(dst_curs, tbl)
        field_list = []
        for f in self.pkey_list:
            field_list.append(f)
        for f in src_cols:
            if f in self.pkey_list:
                continue
            if f in dst_cols:
                field_list.append(f)

        self.common_fields = field_list

        fqlist = [skytools.quote_ident(col) for col in field_list]

        tbl_expr = "%s (%s)" % (skytools.quote_fqident(tbl), ",".join(fqlist))

        self.log.debug("using copy expr: %s" % tbl_expr)

        return tbl_expr

    def dump_table(self, tbl, copy_tbl, curs, fn):
        f = open(fn, "w", 64*1024)
        curs.copy_to(f, copy_tbl)
        size = f.tell()
        f.close()
        self.log.info('Got %d bytes' % size)

    def get_row(self, ln):
        if not ln:
            return None
        t = ln[:-1].split('\t')
        row = {}
        for i in range(len(self.common_fields)):
            row[self.common_fields[i]] = t[i]
        return row

    def dump_compare(self, tbl, src_fn, dst_fn):
        self.log.info("Comparing dumps: %s" % tbl)
        self.cnt_insert = 0
        self.cnt_update = 0
        self.cnt_delete = 0
        self.total_src = 0
        self.total_dst = 0
        f1 = open(src_fn, "r", 64*1024)
        f2 = open(dst_fn, "r", 64*1024)
        src_ln = f1.readline()
        dst_ln = f2.readline()
        if src_ln: self.total_src += 1
        if dst_ln: self.total_dst += 1

        fix = "fix.%s.sql" % tbl
        if os.path.isfile(fix):
            os.unlink(fix)

        while src_ln or dst_ln:
            keep_src = keep_dst = 0
            if src_ln != dst_ln:
                src_row = self.get_row(src_ln)
                dst_row = self.get_row(dst_ln)

                cmp = self.cmp_keys(src_row, dst_row)
                if cmp > 0:
                    # src > dst
                    self.got_missed_delete(tbl, dst_row)
                    keep_src = 1
                elif cmp < 0:
                    # src < dst
                    self.got_missed_insert(tbl, src_row)
                    keep_dst = 1
                else:
                    if self.cmp_data(src_row, dst_row) != 0:
                        self.got_missed_update(tbl, src_row, dst_row)

            if not keep_src:
                src_ln = f1.readline()
                if src_ln: self.total_src += 1
            if not keep_dst:
                dst_ln = f2.readline()
                if dst_ln: self.total_dst += 1

        self.log.info("finished %s: src: %d rows, dst: %d rows,"\
                    " missed: %d inserts, %d updates, %d deletes" % (
                tbl, self.total_src, self.total_dst,
                self.cnt_insert, self.cnt_update, self.cnt_delete))

    def got_missed_insert(self, tbl, src_row):
        self.cnt_insert += 1
        fld_list = self.common_fields
        fq_list = []
        val_list = []
        for f in fld_list:
            fq_list.append(skytools.quote_ident(f))
            v = unescape(src_row[f])
            val_list.append(skytools.quote_literal(v))
        q = "insert into %s (%s) values (%s);" % (
                tbl, ", ".join(fq_list), ", ".join(val_list))
        self.show_fix(tbl, q, 'insert')

    def got_missed_update(self, tbl, src_row, dst_row):
        self.cnt_update += 1
        fld_list = self.common_fields
        set_list = []
        whe_list = []
        for f in self.pkey_list:
            self.addcmp(whe_list, skytools.quote_ident(f), unescape(src_row[f]))
        for f in fld_list:
            v1 = src_row[f]
            v2 = dst_row[f]
            if self.cmp_value(v1, v2) == 0:
                continue

            self.addeq(set_list, skytools.quote_ident(f), unescape(v1))
            self.addcmp(whe_list, skytools.quote_ident(f), unescape(v2))

        q = "update only %s set %s where %s;" % (
                tbl, ", ".join(set_list), " and ".join(whe_list))
        self.show_fix(tbl, q, 'update')

    def got_missed_delete(self, tbl, dst_row):
        self.cnt_delete += 1
        whe_list = []
        for f in self.pkey_list:
            self.addcmp(whe_list, skytools.quote_ident(f), unescape(dst_row[f]))
        q = "delete from only %s where %s;" % (skytools.quote_fqident(tbl), " and ".join(whe_list))
        self.show_fix(tbl, q, 'delete')

    def show_fix(self, tbl, q, desc):
        #self.log.warning("missed %s: %s" % (desc, q))
        fn = "fix.%s.sql" % tbl
        open(fn, "a").write("%s\n" % q)

    def addeq(self, list, f, v):
        vq = skytools.quote_literal(v)
        s = "%s = %s" % (f, vq)
        list.append(s)

    def addcmp(self, list, f, v):
        if v is None:
            s = "%s is null" % f
        else:
            vq = skytools.quote_literal(v)
            s = "%s = %s" % (f, vq)
        list.append(s)

    def cmp_data(self, src_row, dst_row):
        for k in self.common_fields:
            v1 = src_row[k]
            v2 = dst_row[k]
            if self.cmp_value(v1, v2) != 0:
                return -1
        return 0

    def cmp_value(self, v1, v2):
        if v1 == v2:
            return 0

        # try to work around tz vs. notz
        z1 = len(v1)
        z2 = len(v2)
        if z1 == z2 + 3 and z2 >= 19 and v1[z2] == '+':
            v1 = v1[:-3]
            if v1 == v2:
                return 0
        elif z1 + 3 == z2 and z1 >= 19 and v2[z1] == '+':
            v2 = v2[:-3]
            if v1 == v2:
                return 0

        return -1

    def cmp_keys(self, src_row, dst_row):
        """Compare primary keys of the rows.
        
        Returns 1 if src > dst, -1 if src < dst and 0 if src == dst"""

        # None means table is done.  tag it larger than any existing row.
        if src_row is None:
            if dst_row is None:
                return 0
            return 1
        elif dst_row is None:
            return -1

        for k in self.pkey_list:
            v1 = src_row[k]
            v2 = dst_row[k]
            if v1 < v2:
                return -1
            elif v1 > v2:
                return 1
        return 0

