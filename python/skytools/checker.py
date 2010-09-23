
"""Catch moment when tables are in sync on master and slave.
"""

import sys, time, os, subprocess

import pkgloader
pkgloader.require('skytools', '3.0')
import skytools

class TableRepair:
    """Checks that tables in two databases are in sync."""

    def __init__(self, table_name, log):
        self.table_name = table_name
        self.fq_table_name = skytools.quote_fqident(table_name)
        self.log = log
        self.reset()

    def reset(self):
        self.cnt_insert = 0
        self.cnt_update = 0
        self.cnt_delete = 0
        self.total_src = 0
        self.total_dst = 0
        self.pkey_list = []
        self.common_fields = []

    def do_repair(self, src_db, dst_db, where, pfx = 'repair', apply_fixes = False):
        """Actual comparision."""

        self.reset()

        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        self.log.info('Checking %s' % self.table_name)

        copy_tbl = self.gen_copy_tbl(src_curs, dst_curs, where)

        dump_src = "%s.%s.src" % (pfx, self.table_name)
        dump_dst = "%s.%s.dst" % (pfx, self.table_name)
        fix = "%s.%s.fix" % (pfx, self.table_name)

        self.log.info("Dumping src table: %s" % self.table_name)
        self.dump_table(copy_tbl, src_curs, dump_src)
        src_db.commit()
        self.log.info("Dumping dst table: %s" % self.table_name)
        self.dump_table(copy_tbl, dst_curs, dump_dst)
        dst_db.commit()
        
        self.log.info("Sorting src table: %s" % self.table_name)
        self.do_sort(dump_src, dump_src + '.sorted')

        self.log.info("Sorting dst table: %s" % self.table_name)
        self.do_sort(dump_dst, dump_dst + '.sorted')

        self.dump_compare(dump_src + ".sorted", dump_dst + ".sorted", fix)

        os.unlink(dump_src)
        os.unlink(dump_dst)
        os.unlink(dump_src + ".sorted")
        os.unlink(dump_dst + ".sorted")

        if apply_fixes:
            pass

    def do_sort(self, src, dst):
        p = subprocess.Popen(["sort", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_ver = p.communicate()[0]
        del p

        xenv = os.environ.copy()
        xenv['LANG'] = 'C'
        xenv['LC_ALL'] = 'C'

        cmdline = ['sort', '-T', '.']
        if s_ver.find("coreutils") > 0:
            cmdline.append('-S')
            cmdline.append('30%')
        cmdline.append('-o')
        cmdline.append(dst)
        cmdline.append(src)
        p = subprocess.Popen(cmdline, env = xenv)
        if p.wait() != 0:
            raise Exception('sort failed')

    def gen_copy_tbl(self, src_curs, dst_curs, where):
        """Create COPY expession from common fields."""
        self.pkey_list = skytools.get_table_pkeys(src_curs, self.table_name)
        dst_pkey = skytools.get_table_pkeys(dst_curs, self.table_name)
        if dst_pkey != self.pkey_list:
            self.log.error('pkeys do not match')
            sys.exit(1)

        src_cols = skytools.get_table_columns(src_curs, self.table_name)
        dst_cols = skytools.get_table_columns(dst_curs, self.table_name)
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

        tbl_expr = "select %s from %s" % (",".join(fqlist), self.fq_table_name)
        if where:
            tbl_expr += ' where ' + where
        tbl_expr = "COPY (%s) TO STDOUT" % tbl_expr

        self.log.debug("using copy expr: %s" % tbl_expr)

        return tbl_expr

    def dump_table(self, copy_cmd, curs, fn):
        """Dump table to disk."""
        f = open(fn, "w", 64*1024)
        curs.copy_expert(f, copy_cmd)
        self.log.info('%s: Got %d bytes' % (self.table_name, f.tell()))
        f.close()

    def get_row(self, ln):
        """Parse a row into dict."""
        if not ln:
            return None
        t = ln[:-1].split('\t')
        row = {}
        for i in range(len(self.common_fields)):
            row[self.common_fields[i]] = t[i]
        return row

    def dump_compare(self, src_fn, dst_fn, fix):
        """Dump + compare single table."""
        self.log.info("Comparing dumps: %s" % self.table_name)
        f1 = open(src_fn, "r", 64*1024)
        f2 = open(dst_fn, "r", 64*1024)
        src_ln = f1.readline()
        dst_ln = f2.readline()
        if src_ln: self.total_src += 1
        if dst_ln: self.total_dst += 1

        if os.path.isfile(fix):
            os.unlink(fix)

        while src_ln or dst_ln:
            keep_src = keep_dst = 0
            if src_ln != dst_ln:
                src_row = self.get_row(src_ln)
                dst_row = self.get_row(dst_ln)

                diff = self.cmp_keys(src_row, dst_row)
                if diff > 0:
                    # src > dst
                    self.got_missed_delete(dst_row, fix)
                    keep_src = 1
                elif diff < 0:
                    # src < dst
                    self.got_missed_insert(src_row, fix)
                    keep_dst = 1
                else:
                    if self.cmp_data(src_row, dst_row) != 0:
                        self.got_missed_update(src_row, dst_row, fix)

            if not keep_src:
                src_ln = f1.readline()
                if src_ln: self.total_src += 1
            if not keep_dst:
                dst_ln = f2.readline()
                if dst_ln: self.total_dst += 1

        self.log.info("finished %s: src: %d rows, dst: %d rows,"\
                    " missed: %d inserts, %d updates, %d deletes" % (
                self.table_name, self.total_src, self.total_dst,
                self.cnt_insert, self.cnt_update, self.cnt_delete))

    def got_missed_insert(self, src_row, fn):
        """Create sql for missed insert."""
        self.cnt_insert += 1
        fld_list = self.common_fields
        fq_list = []
        val_list = []
        for f in fld_list:
            fq_list.append(skytools.quote_ident(f))
            v = skytools.unescape_copy(src_row[f])
            val_list.append(skytools.quote_literal(v))
        q = "insert into %s (%s) values (%s);" % (
                self.fq_table_name, ", ".join(fq_list), ", ".join(val_list))
        self.show_fix(q, 'insert', fn)

    def got_missed_update(self, src_row, dst_row, fn):
        """Create sql for missed update."""
        self.cnt_update += 1
        fld_list = self.common_fields
        set_list = []
        whe_list = []
        for f in self.pkey_list:
            self.addcmp(whe_list, skytools.quote_ident(f), skytools.unescape_copy(src_row[f]))
        for f in fld_list:
            v1 = src_row[f]
            v2 = dst_row[f]
            if self.cmp_value(v1, v2) == 0:
                continue

            self.addeq(set_list, skytools.quote_ident(f), skytools.unescape_copy(v1))
            self.addcmp(whe_list, skytools.quote_ident(f), skytools.unescape_copy(v2))

        q = "update only %s set %s where %s;" % (
                self.fq_table_name, ", ".join(set_list), " and ".join(whe_list))
        self.show_fix(q, 'update', fn)

    def got_missed_delete(self, dst_row, fn):
        """Create sql for missed delete."""
        self.cnt_delete += 1
        whe_list = []
        for f in self.pkey_list:
            self.addcmp(whe_list, skytools.quote_ident(f), skytools.unescape_copy(dst_row[f]))
        q = "delete from only %s where %s;" % (self.fq_table_name, " and ".join(whe_list))
        self.show_fix(q, 'delete', fn)

    def show_fix(self, q, desc, fn):
        """Print/write/apply repair sql."""
        self.log.debug("missed %s: %s" % (desc, q))
        open(fn, "a").write("%s\n" % q)

    def addeq(self, list, f, v):
        """Add quoted SET."""
        vq = skytools.quote_literal(v)
        s = "%s = %s" % (f, vq)
        list.append(s)

    def addcmp(self, list, f, v):
        """Add quoted comparision."""
        if v is None:
            s = "%s is null" % f
        else:
            vq = skytools.quote_literal(v)
            s = "%s = %s" % (f, vq)
        list.append(s)

    def cmp_data(self, src_row, dst_row):
        """Compare data field-by-field."""
        for k in self.common_fields:
            v1 = src_row[k]
            v2 = dst_row[k]
            if self.cmp_value(v1, v2) != 0:
                return -1
        return 0

    def cmp_value(self, v1, v2):
        """Compare single field, tolerates tz vs notz dates."""
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

