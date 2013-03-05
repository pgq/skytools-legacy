#! /usr/bin/env python

"""Load data from queue into tables, with optional partitioning.

Config template::

    [queue_loader]
    job_name =
    logfile =
    pidfile =

    db =

    #rename_tables =

    [DEFAULT]

    # fields - which fields to send through
    #fields = col1, col2, col3:renamed3
    #fields = *

    # table_mode - how to handle a table
    #
    # ignore - ignore this table
    # direct - update table directly
    # split - split data into partitions
    #table_mode = ignore

    # split_mode - how to split, if requested
    #
    # by-batch-time: use batch time for splitting
    # by-event-time: use event time for splitting
    # by-date-field:fld - use fld for splitting
    #split_mode = by-batch-time

    # split_part - partition name format
    #
    # %(table_name)s %(year)s %(month)s %(day)s %(hour)s
    #split_part = %(table_name)s_%(year)s_%(month)s_%(day)s

    # split_part_template - How to create new partition tables
    #
    # Available fields:
    # %(part)s
    # %(parent)s
    # %(pkey)s
    #
    ### Non-inherited partitions
    #split_part_template =
    #    create table %%(part)s (like %%(parent)s);
    #    alter table only %%(part)s add primary key (%%(pkey)s);
    #
    ### Inherited partitions
    #split_part_template =
    #    create table %%(part)s () inherits (%%(parent)s);
    #    alter table only %%(part)s add primary key (%%(pkey)s);


    # row_mode - How to apply the events
    #
    # plain - each event creates SQL statement to run
    # keep_latest - change updates to DELETE + INSERT
    # keep_all - change updates to inserts, ignore deletes
    # bulk - instead of statement-per-row, do bulk updates
    #row_mode = plain


    # bulk_mode - How to do the bulk update
    #
    # correct - inserts as COPY into table,
    #           update as COPY into temp table and single UPDATE from there
    #           delete as COPY into temp table and single DELETE from there
    # delete - as 'correct', but do update as DELETE + COPY
    # merged - as 'delete', but merge insert rows with update rows
    #bulk_mode=correct

    [table public.foo]
    mode =
    create_sql =
"""

import sys, time

import pkgloader
pkgloader.require('skytools', '3.0')

import skytools
from pgq.cascade.worker import CascadedWorker
from skytools import quote_ident, quote_fqident, UsageError

# TODO: auto table detect

# BulkLoader load method
METH_CORRECT = 0
METH_DELETE = 1
METH_MERGED = 2
LOAD_METHOD = METH_CORRECT
# BulkLoader hacks
AVOID_BIZGRES_BUG = 0
USE_LONGLIVED_TEMP_TABLES = True


class BasicLoader:
    """Apply events as-is."""
    def __init__(self, table_name, parent_name, log):
        self.table_name = table_name
        self.parent_name = parent_name
        self.sql_list = []
        self.log = log

    def add_row(self, op, data, pkey_list):
        if op == 'I':
            sql = skytools.mk_insert_sql(data, self.table_name, pkey_list)
        elif op == 'U':
            sql = skytools.mk_update_sql(data, self.table_name, pkey_list)
        elif op == 'D':
            sql = skytools.mk_delete_sql(data, self.table_name, pkey_list)
        else:
            raise Exception('bad operation: '+op)
        self.sql_list.append(sql)

    def flush(self, curs):
        if len(self.sql_list) > 0:
            curs.execute("\n".join(self.sql_list))
            self.sql_list = []


class KeepLatestLoader(BasicLoader):
    """Keep latest row version.

    Updates are changed to delete + insert, deletes are ignored.
    Makes sense only for partitioned tables.
    """
    def add_row(self, op, data, pkey_list):
        if op == 'U':
            BasicLoader.add_row(self, 'D', data, pkey_list)
            BasicLoader.add_row(self, 'I', data, pkey_list)
        elif op == 'I':
            BasicLoader.add_row(self, 'I', data, pkey_list)
        else:
            pass


class KeepAllLoader(BasicLoader):
    """Keep all row versions.

    Updates are changed to inserts, deletes are ignored.
    Makes sense only for partitioned tables.
    """
    def add_row(self, op, data, pkey_list):
        if op == 'U':
            op = 'I'
        elif op == 'D':
            return
        BasicLoader.add_row(self, op, data, pkey_list)


class BulkEvent(object):
    """Helper class for BulkLoader to store relevant data."""
    __slots__ = ('op', 'data', 'pk_data')
    def __init__(self, op, data, pk_data):
        self.op = op
        self.data = data
        self.pk_data = pk_data


class BulkLoader(BasicLoader):
    """Instead of statement-per event, load all data with one
    big COPY, UPDATE or DELETE statement.
    """
    fake_seq = 0
    def __init__(self, table_name, parent_name, log):
        """Init per-batch table data cache."""
        BasicLoader.__init__(self, table_name, parent_name, log)

        self.pkey_list = None
        self.dist_fields = None
        self.col_list = None

        self.ev_list = []
        self.pkey_ev_map = {}

    def reset(self):
        self.ev_list = []
        self.pkey_ev_map = {}

    def add_row(self, op, data, pkey_list):
        """Store new event."""

        # get pkey value
        if self.pkey_list is None:
            self.pkey_list = pkey_list
        if len(self.pkey_list) > 0:
            pk_data = (data[k] for k in self.pkey_list)
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

        # add to list
        ev = BulkEvent(op, data, pk_data)
        self.ev_list.append(ev)

        # keep all versions of row data
        if ev.pk_data in self.pkey_ev_map:
            self.pkey_ev_map[ev.pk_data].append(ev)
        else:
            self.pkey_ev_map[ev.pk_data] = [ev]

    def prepare_data(self):
        """Got all data, prepare for insertion."""

        del_list = []
        ins_list = []
        upd_list = []
        for ev_list in self.pkey_ev_map.values():
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

    def flush(self, curs):
        ins_list, upd_list, del_list = self.prepare_data()

        # reorder cols
        col_list = self.pkey_list[:]
        for k in self.col_list:
            if k not in self.pkey_list:
                col_list.append(k)

        real_update_count = len(upd_list)

        #self.log.debug("process_one_table: %s  (I/U/D = %d/%d/%d)",
        #               tbl, len(ins_list), len(upd_list), len(del_list))

        # hack to unbroke stuff
        if LOAD_METHOD == METH_MERGED:
            upd_list += ins_list
            ins_list = []

        # fetch distribution fields
        if self.dist_fields is None:
            self.dist_fields = self.find_dist_fields(curs)

        key_fields = self.pkey_list[:]
        for fld in self.dist_fields:
            if fld not in key_fields:
                key_fields.append(fld)
        #self.log.debug("PKey fields: %s  Extra fields: %s",
        #               ",".join(cache.pkey_list), ",".join(extra_fields))

        # create temp table
        temp = self.create_temp_table(curs)
        tbl = self.table_name

        # where expr must have pkey and dist fields
        klist = []
        for pk in key_fields:
            exp = "%s.%s = %s.%s" % (quote_fqident(tbl), quote_ident(pk),
                                     quote_fqident(temp), quote_ident(pk))
            klist.append(exp)
        whe_expr = " and ".join(klist)

        # create del sql
        del_sql = "delete from only %s using %s where %s" % (
                  quote_fqident(tbl), quote_fqident(temp), whe_expr)

        # create update sql
        slist = []
        for col in col_list:
            if col not in key_fields:
                exp = "%s = %s.%s" % (quote_ident(col), quote_fqident(temp), quote_ident(col))
                slist.append(exp)
        upd_sql = "update only %s set %s from %s where %s" % (
                    quote_fqident(tbl), ", ".join(slist), quote_fqident(temp), whe_expr)

        # insert sql
        colstr = ",".join([quote_ident(c) for c in col_list])
        ins_sql = "insert into %s (%s) select %s from %s" % (
                  quote_fqident(tbl), colstr, colstr, quote_fqident(temp))

        temp_used = False

        # process deleted rows
        if len(del_list) > 0:
            #self.log.info("Deleting %d rows from %s", len(del_list), tbl)
            # delete old rows
            q = "truncate %s" % quote_fqident(temp)
            self.log.debug(q)
            curs.execute(q)
            # copy rows
            self.log.debug("COPY %d rows into %s", len(del_list), temp)
            skytools.magic_insert(curs, temp, del_list, col_list)
            # delete rows
            self.log.debug(del_sql)
            curs.execute(del_sql)
            self.log.debug("%s - %d", curs.statusmessage, curs.rowcount)
            if len(del_list) != curs.rowcount:
                self.log.warning("Delete mismatch: expected=%d deleted=%d",
                                 len(del_list), curs.rowcount)
            temp_used = True

        # process updated rows
        if len(upd_list) > 0:
            #self.log.info("Updating %d rows in %s", len(upd_list), tbl)
            # delete old rows
            q = "truncate %s" % quote_fqident(temp)
            self.log.debug(q)
            curs.execute(q)
            # copy rows
            self.log.debug("COPY %d rows into %s", len(upd_list), temp)
            skytools.magic_insert(curs, temp, upd_list, col_list)
            temp_used = True
            if LOAD_METHOD == METH_CORRECT:
                # update main table
                self.log.debug(upd_sql)
                curs.execute(upd_sql)
                self.log.debug("%s - %d", curs.statusmessage, curs.rowcount)
                # check count
                if len(upd_list) != curs.rowcount:
                    self.log.warning("Update mismatch: expected=%d updated=%d",
                                     len(upd_list), curs.rowcount)
            else:
                # delete from main table
                self.log.debug(del_sql)
                curs.execute(del_sql)
                self.log.debug(curs.statusmessage)
                # check count
                if real_update_count != curs.rowcount:
                    self.log.warning("Update mismatch: expected=%d deleted=%d",
                                     real_update_count, curs.rowcount)
                # insert into main table
                if AVOID_BIZGRES_BUG:
                    # copy again, into main table
                    self.log.debug("COPY %d rows into %s", len(upd_list), tbl)
                    skytools.magic_insert(curs, tbl, upd_list, col_list)
                else:
                    # better way, but does not work due bizgres bug
                    self.log.debug(ins_sql)
                    curs.execute(ins_sql)
                    self.log.debug(curs.statusmessage)

        # process new rows
        if len(ins_list) > 0:
            self.log.info("Inserting %d rows into %s", len(ins_list), tbl)
            skytools.magic_insert(curs, tbl, ins_list, col_list)

        # delete remaining rows
        if temp_used:
            if USE_LONGLIVED_TEMP_TABLES:
                q = "truncate %s" % quote_fqident(temp)
            else:
                # fscking problems with long-lived temp tables
                q = "drop table %s" % quote_fqident(temp)
            self.log.debug(q)
            curs.execute(q)

        self.reset()

    def create_temp_table(self, curs):
        # create temp table for loading
        tempname = self.table_name.replace('.', '_') + "_loadertmp"

        # check if exists
        if USE_LONGLIVED_TEMP_TABLES:
            if skytools.exists_temp_table(curs, tempname):
                self.log.debug("Using existing temp table %s", tempname)
                return tempname

        # bizgres crashes on delete rows
        arg = "on commit delete rows"
        arg = "on commit preserve rows"
        # create temp table for loading
        q = "create temp table %s (like %s) %s" % (
                quote_fqident(tempname), quote_fqident(self.table_name), arg)
        self.log.debug("Creating temp table: %s", q)
        curs.execute(q)
        return tempname

    def find_dist_fields(self, curs):
        if not skytools.exists_table(curs, "pg_catalog.mpp_distribution_policy"):
            return []
        schema, name = skytools.fq_name_parts(self.table_name)
        q = "select a.attname"\
            "  from pg_class t, pg_namespace n, pg_attribute a,"\
            "       mpp_distribution_policy p"\
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


class TableHandler:
    """Basic partitioned loader.
    Splits events into partitions, if requested.
    Then applies them without further processing.
    """
    def __init__(self, rowhandler, table_name, table_mode, cf, log):
        self.part_map = {}
        self.rowhandler = rowhandler
        self.table_name = table_name
        self.quoted_name = quote_fqident(table_name)
        self.log = log
        if table_mode == 'direct':
            self.split = False
        elif table_mode == 'split':
            self.split = True
            smode = cf.get('split_mode', 'by-batch-time')
            sfield = None
            if smode.find(':') > 0:
                smode, sfield = smode.split(':', 1)
            self.split_field = sfield
            self.split_part = cf.get('split_part', '%(table_name)s_%(year)s_%(month)s_%(day)s')
            self.split_part_template = cf.get('split_part_template', '')
            if smode == 'by-batch-time':
                self.split_format = self.split_date_from_batch
            elif smode == 'by-event-time':
                self.split_format = self.split_date_from_event
            elif smode == 'by-date-field':
                self.split_format = self.split_date_from_field
            else:
                raise UsageError('Bad value for split_mode: '+smode)
            self.log.debug("%s: split_mode=%s, split_field=%s, split_part=%s",
                    self.table_name, smode, self.split_field, self.split_part)
        elif table_mode == 'ignore':
            pass
        else:
            raise UsageError('Bad value for table_mode: '+table_mode)

    def split_date_from_batch(self, ev, data, batch_info):
        d = batch_info['batch_end']
        vals = {
            'table_name': self.table_name,
            'year': "%04d" % d.year,
            'month': "%02d" % d.month,
            'day': "%02d" % d.day,
            'hour': "%02d" % d.hour,
        }
        dst = self.split_part % vals
        return dst

    def split_date_from_event(self, ev, data, batch_info):
        d = ev.ev_date
        vals = {
            'table_name': self.table_name,
            'year': "%04d" % d.year,
            'month': "%02d" % d.month,
            'day': "%02d" % d.day,
            'hour': "%02d" % d.hour,
        }
        dst = self.split_part % vals
        return dst

    def split_date_from_field(self, ev, data, batch_info):
        val = data[self.split_field]
        date, time = val.split(' ', 1)
        y, m, d = date.split('-')
        h, rest = time.split(':', 1)
        vals = {
            'table_name': self.table_name,
            'year': y,
            'month': m,
            'day': d,
            'hour': h,
        }
        dst = self.split_part % vals
        return dst

    def add(self, curs, ev, batch_info):
        data = skytools.db_urldecode(ev.data)
        op, pkeys = ev.type.split(':', 1)
        pkey_list = pkeys.split(',')
        if self.split:
            dst = self.split_format(ev, data, batch_info)
            if dst not in self.part_map:
                self.check_part(curs, dst, pkey_list)
        else:
            dst = self.table_name

        if dst not in self.part_map:
            self.part_map[dst] = self.rowhandler(dst, self.table_name, self.log)

        p = self.part_map[dst]
        p.add_row(op, data, pkey_list)

    def flush(self, curs):
        for part in self.part_map.values():
            part.flush(curs)

    def check_part(self, curs, dst, pkey_list):
        if skytools.exists_table(curs, dst):
            return
        if not self.split_part_template:
            raise UsageError('Partition %s does not exist and split_part_template not specified' % dst)

        vals = {
            'dest': quote_fqident(dst),
            'part': quote_fqident(dst),
            'parent': quote_fqident(self.table_name),
            'pkey': ",".join(pkey_list), # quoting?
        }
        sql = self.split_part_template % vals
        curs.execute(sql)


class IgnoreTable(TableHandler):
    """Do-nothing."""
    def add(self, curs, ev, batch_info):
        pass


class QueueLoader(CascadedWorker):
    """Loader script."""
    table_state = {}

    def reset(self):
        """Drop our caches on error."""
        self.table_state = {}
        CascadedWorker.reset(self)

    def init_state(self, tbl):
        cf = self.cf
        if tbl in cf.cf.sections():
            cf = cf.clone(tbl)
        table_mode = cf.get('table_mode', 'ignore')
        row_mode = cf.get('row_mode', 'plain')
        if table_mode == 'ignore':
            tblhandler = IgnoreTable
        else:
            tblhandler = TableHandler

        if row_mode == 'plain':
            rowhandler = BasicLoader
        elif row_mode == 'keep_latest':
            rowhandler = KeepLatestLoader
        elif row_mode == 'keep_all':
            rowhandler = KeepAllLoader
        elif row_mode == 'bulk':
            rowhandler = BulkLoader
        else:
            raise UsageError('Bad row_mode: '+row_mode)
        self.table_state[tbl] = tblhandler(rowhandler, tbl, table_mode, cf, self.log)

    def process_remote_event(self, src_curs, dst_curs, ev):
        t = ev.type[:2]
        if t not in ('I:', 'U:', 'D:'):
            CascadedWorker.process_remote_event(self, src_curs, dst_curs, ev)
            return

        tbl = ev.extra1
        if tbl not in self.table_state:
            self.init_state(tbl)
        st = self.table_state[tbl]
        st.add(dst_curs, ev, self._batch_info)

    def finish_remote_batch(self, src_db, dst_db, tick_id):
        curs = dst_db.cursor()
        for st in self.table_state.values():
            st.flush(curs)
        CascadedWorker.finish_remote_batch(self, src_db, dst_db, tick_id)


if __name__ == '__main__':
    script = QueueLoader('queue_loader', 'db', sys.argv[1:])
    script.start()

