"""
Bulk loading into OLAP database.

To use set in londiste.ini:

    handler_modules = londiste.handlers.bulk

then add table with:
  londiste3 add-table xx --handler="bulk"

or:
  londiste3 add-table xx --handler="bulk(method=X)"

Methods:

  0 (correct) - inserts as COPY into table,
                update as COPY into temp table and single UPDATE from there
                delete as COPY into temp table and single DELETE from there
  1 (delete)  - as 'correct', but do update as DELETE + COPY
  2 (merged)  - as 'delete', but merge insert rows with update rows

Default is 0.

"""

import skytools

from londiste.handler import BaseHandler, RowCache
from skytools import quote_ident, quote_fqident

__all__ = ['BulkLoader']

# BulkLoader load method
METH_CORRECT = 0
METH_DELETE = 1
METH_MERGED = 2
DEFAULT_METHOD = METH_CORRECT

# BulkLoader hacks
AVOID_BIZGRES_BUG = 0
USE_LONGLIVED_TEMP_TABLES = True

USE_REAL_TABLE = False

class BulkEvent(object):
    """Helper class for BulkLoader to store relevant data."""
    __slots__ = ('op', 'data', 'pk_data')
    def __init__(self, op, data, pk_data):
        self.op = op
        self.data = data
        self.pk_data = pk_data

class BulkLoader(BaseHandler):
    """Bulk loading into OLAP database.
    Instead of statement-per-event, load all data with one big COPY, UPDATE
    or DELETE statement.

    Parameters:
      method=TYPE - method to use for copying [0..2] (default: 0)

    Methods:
      0 (correct) - inserts as COPY into table,
                    update as COPY into temp table and single UPDATE from there
                    delete as COPY into temp table and single DELETE from there
      1 (delete)  - as 'correct', but do update as DELETE + COPY
      2 (merged)  - as 'delete', but merge insert rows with update rows
    """
    handler_name = 'bulk'
    fake_seq = 0

    def __init__(self, table_name, args, dest_table):
        """Init per-batch table data cache."""

        BaseHandler.__init__(self, table_name, args, dest_table)

        self.pkey_list = None
        self.dist_fields = None
        self.col_list = None

        self.pkey_ev_map = {}
        self.method = int(args.get('method', DEFAULT_METHOD))
        if not self.method in (0,1,2):
            raise Exception('unknown method: %s' % self.method)

        self.log.debug('bulk_init(%s), method=%d' % (repr(args), self.method))

    def reset(self):
        self.pkey_ev_map = {}
        BaseHandler.reset(self)

    def finish_batch(self, batch_info, dst_curs):
        self.bulk_flush(dst_curs)

    def process_event(self, ev, sql_queue_func, arg):
        if len(ev.ev_type) < 2 or ev.ev_type[1] != ':':
            raise Exception('Unsupported event type: %s/extra1=%s/data=%s' % (
                            ev.ev_type, ev.ev_extra1, ev.ev_data))
        op = ev.ev_type[0]
        if op not in 'IUD':
            raise Exception('Unknown event type: '+ev.ev_type)
        self.log.debug('bulk.process_event: %s/%s' % (ev.ev_type, ev.ev_data))
        # pkey_list = ev.ev_type[2:].split(',')
        data = skytools.db_urldecode(ev.ev_data)

        # get pkey value
        if self.pkey_list is None:
            #self.pkey_list = pkey_list
            self.pkey_list = ev.ev_type[2:].split(',')
        if len(self.pkey_list) > 0:
            pk_data = tuple(data[k] for k in self.pkey_list)
        elif op == 'I':
            # fake pkey, just to get them spread out
            pk_data = self.fake_seq
            self.fake_seq += 1
        else:
            raise Exception('non-pk tables not supported: %s' % self.table_name)

        # get full column list, detect added columns
        if not self.col_list:
            self.col_list = data.keys()
        elif self.col_list != data.keys():
            # ^ supposedly python guarantees same order in keys()
            self.col_list = data.keys()

        # keep all versions of row data
        ev = BulkEvent(op, data, pk_data)
        if ev.pk_data in self.pkey_ev_map:
            self.pkey_ev_map[ev.pk_data].append(ev)
        else:
            self.pkey_ev_map[ev.pk_data] = [ev]

    def prepare_data(self):
        """Got all data, prepare for insertion."""

        del_list = []
        ins_list = []
        upd_list = []
        for ev_list in self.pkey_ev_map.itervalues():
            # rewrite list of I/U/D events to
            # optional DELETE and optional INSERT/COPY command
            exists_before = -1
            exists_after = 1
            for ev in ev_list:
                if ev.op == "I":
                    if exists_before < 0:
                        exists_before = 0
                    exists_after = 1
                elif ev.op == "U":
                    if exists_before < 0:
                        exists_before = 1
                    #exists_after = 1 # this shouldnt be needed
                elif ev.op == "D":
                    if exists_before < 0:
                        exists_before = 1
                    exists_after = 0
                else:
                    raise Exception('unknown event type: %s' % ev.op)

            # skip short-lived rows
            if exists_before == 0 and exists_after == 0:
                continue

            # take last event
            ev = ev_list[-1]

            # generate needed commands
            if exists_before and exists_after:
                upd_list.append(ev.data)
            elif exists_before:
                del_list.append(ev.data)
            elif exists_after:
                ins_list.append(ev.data)

        return ins_list, upd_list, del_list

    def bulk_flush(self, curs):
        ins_list, upd_list, del_list = self.prepare_data()

        # reorder cols, put pks first
        col_list = self.pkey_list[:]
        for k in self.col_list:
            if k not in self.pkey_list:
                col_list.append(k)

        real_update_count = len(upd_list)

        self.log.debug("bulk_flush: %s  (I/U/D = %d/%d/%d)" % (
                       self.table_name, len(ins_list), len(upd_list), len(del_list)))

        # hack to unbroke stuff
        if self.method == METH_MERGED:
            upd_list += ins_list
            ins_list = []

        # fetch distribution fields
        if self.dist_fields is None:
            self.dist_fields = self.find_dist_fields(curs)

        key_fields = self.pkey_list[:]
        for fld in self.dist_fields:
            if fld not in key_fields:
                key_fields.append(fld)
        self.log.debug("PKey fields: %s  Dist fields: %s" % (
                       ",".join(self.pkey_list), ",".join(self.dist_fields)))

        # create temp table
        temp, qtemp = self.create_temp_table(curs)
        tbl = self.dest_table
        qtbl = self.fq_dest_table

        # where expr must have pkey and dist fields
        klist = []
        for pk in key_fields:
            exp = "%s.%s = %s.%s" % (qtbl, quote_ident(pk),
                                     qtemp, quote_ident(pk))
            klist.append(exp)
        whe_expr = " and ".join(klist)

        # create del sql
        del_sql = "delete from only %s using %s where %s" % (qtbl, qtemp, whe_expr)

        # create update sql
        slist = []
        for col in col_list:
            if col not in key_fields:
                exp = "%s = %s.%s" % (quote_ident(col), qtemp, quote_ident(col))
                slist.append(exp)
        upd_sql = "update only %s set %s from %s where %s" % (
                   qtbl, ", ".join(slist), qtemp, whe_expr)

        # avoid updates on pk-only table
        if not slist:
            upd_list = []

        # insert sql
        colstr = ",".join([quote_ident(c) for c in col_list])
        ins_sql = "insert into %s (%s) select %s from %s" % (
                  qtbl, colstr, colstr, qtemp)

        temp_used = False

        # process deleted rows
        if len(del_list) > 0:
            self.log.debug("bulk: Deleting %d rows from %s" % (len(del_list), tbl))
            # delete old rows
            q = "truncate %s" % qtemp
            self.log.debug('bulk: %s' % q)
            curs.execute(q)
            # copy rows
            self.log.debug("bulk: COPY %d rows into %s" % (len(del_list), temp))
            skytools.magic_insert(curs, qtemp, del_list, col_list, quoted_table=1)
            # delete rows
            self.log.debug('bulk: ' + del_sql)
            curs.execute(del_sql)
            self.log.debug("bulk: %s - %d" % (curs.statusmessage, curs.rowcount))
            if len(del_list) != curs.rowcount:
                self.log.warning("Delete mismatch: expected=%s deleted=%d"
                        % (len(del_list), curs.rowcount))
            temp_used = True

        # process updated rows
        if len(upd_list) > 0:
            self.log.debug("bulk: Updating %d rows in %s" % (len(upd_list), tbl))
            # delete old rows
            q = "truncate %s" % qtemp
            self.log.debug('bulk: ' + q)
            curs.execute(q)
            # copy rows
            self.log.debug("bulk: COPY %d rows into %s" % (len(upd_list), temp))
            skytools.magic_insert(curs, qtemp, upd_list, col_list, quoted_table=1)
            temp_used = True
            if self.method == METH_CORRECT:
                # update main table
                self.log.debug('bulk: ' + upd_sql)
                curs.execute(upd_sql)
                self.log.debug("bulk: %s - %d" % (curs.statusmessage, curs.rowcount))
                # check count
                if len(upd_list) != curs.rowcount:
                    self.log.warning("Update mismatch: expected=%s updated=%d"
                            % (len(upd_list), curs.rowcount))
            else:
                # delete from main table
                self.log.debug('bulk: ' + del_sql)
                curs.execute(del_sql)
                self.log.debug('bulk: ' + curs.statusmessage)
                # check count
                if real_update_count != curs.rowcount:
                    self.log.warning("bulk: Update mismatch: expected=%s deleted=%d"
                            % (real_update_count, curs.rowcount))
                # insert into main table
                if AVOID_BIZGRES_BUG:
                    # copy again, into main table
                    self.log.debug("bulk: COPY %d rows into %s" % (len(upd_list), tbl))
                    skytools.magic_insert(curs, qtbl, upd_list, col_list, quoted_table=1)
                else:
                    # better way, but does not work due bizgres bug
                    self.log.debug('bulk: ' + ins_sql)
                    curs.execute(ins_sql)
                    self.log.debug('bulk: ' + curs.statusmessage)

        # process new rows
        if len(ins_list) > 0:
            self.log.debug("bulk: Inserting %d rows into %s" % (len(ins_list), tbl))
            self.log.debug("bulk: COPY %d rows into %s" % (len(ins_list), tbl))
            skytools.magic_insert(curs, qtbl, ins_list, col_list, quoted_table=1)

        # delete remaining rows
        if temp_used:
            if USE_LONGLIVED_TEMP_TABLES or USE_REAL_TABLE:
                q = "truncate %s" % qtemp
            else:
                # fscking problems with long-lived temp tables
                q = "drop table %s" % qtemp
            self.log.debug('bulk: ' + q)
            curs.execute(q)

        self.reset()

    def create_temp_table(self, curs):
        if USE_REAL_TABLE:
            tempname = self.dest_table + "_loadertmpx"
        else:
            # create temp table for loading
            tempname = self.dest_table.replace('.', '_') + "_loadertmp"

        # check if exists
        if USE_REAL_TABLE:
            if skytools.exists_table(curs, tempname):
                self.log.debug("bulk: Using existing real table %s" % tempname)
                return tempname, quote_fqident(tempname)

            # create non-temp table
            q = "create table %s (like %s)" % (
                        quote_fqident(tempname),
                        quote_fqident(self.dest_table))
            self.log.debug("bulk: Creating real table: %s" % q)
            curs.execute(q)
            return tempname, quote_fqident(tempname)
        elif USE_LONGLIVED_TEMP_TABLES:
            if skytools.exists_temp_table(curs, tempname):
                self.log.debug("bulk: Using existing temp table %s" % tempname)
                return tempname, quote_ident(tempname)

        # bizgres crashes on delete rows
        # removed arg = "on commit delete rows"
        arg = "on commit preserve rows"
        # create temp table for loading
        q = "create temp table %s (like %s) %s" % (
                quote_ident(tempname), quote_fqident(self.dest_table), arg)
        self.log.debug("bulk: Creating temp table: %s" % q)
        curs.execute(q)
        return tempname, quote_ident(tempname)

    def find_dist_fields(self, curs):
        if not skytools.exists_table(curs, "pg_catalog.gp_distribution_policy"):
            return []
        schema, name = skytools.fq_name_parts(self.dest_table)
        q = "select a.attname"\
            "  from pg_class t, pg_namespace n, pg_attribute a,"\
            "       gp_distribution_policy p"\
            " where n.oid = t.relnamespace"\
            "   and p.localoid = t.oid"\
            "   and a.attrelid = t.oid"\
            "   and a.attnum = any(p.attrnums)"\
            "   and n.nspname = %s and t.relname = %s"
        curs.execute(q, [schema, name])
        res = []
        for row in curs.fetchall():
            res.append(row[0])
        return res


# register handler class
__londiste_handlers__ = [BulkLoader]
