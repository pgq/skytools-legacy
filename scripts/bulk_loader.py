#! /usr/bin/env python

"""Bulkloader for slow databases (Bizgres).

Idea is following:
    - Script reads from queue a batch of urlencoded row changes.
      Inserts/updates/deletes, maybe many per one row.
    - It creates 3 lists: ins_list, upd_list, del_list.
      If one row is changed several times, it keeps the latest.
    - Lists are processed in followin way:
      ins_list - COPY into main table
      upd_list - COPY into temp table, UPDATE from there
      del_list - COPY into temp table, DELETE from there
    - One side-effect is that total order of how rows appear
      changes, but per-row changes will be kept in order.

The speedup from the COPY will happen only if the batches are
large enough.  So the ticks should happen only after couple
of minutes.
"""

import sys, os, pgq, skytools
from skytools import quote_ident, quote_fqident


## several methods for applying data

# update as update
METH_CORRECT = 0
# update as delete/copy
METH_DELETE = 1
# merge ins_list and upd_list, do delete/copy
METH_MERGED = 2

# no good method for temp table check before 8.2
USE_LONGLIVED_TEMP_TABLES = False

def find_dist_fields(curs, fqtbl):
    if not skytools.exists_table(curs, "pg_catalog.mpp_distribution_policy"):
        return []
    schema, name = fqtbl.split('.')
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

def exists_temp_table(curs, tbl):
    # correct way, works only on 8.2
    q = "select 1 from pg_class where relname = %s and relnamespace = pg_my_temp_schema()"

    # does not work with parallel case
    #q = """
    #select 1 from pg_class t, pg_namespace n
    #where n.oid = t.relnamespace
    #  and pg_table_is_visible(t.oid)
    #  and has_schema_privilege(n.nspname, 'USAGE')
    #  and has_table_privilege(n.nspname || '.' || t.relname, 'SELECT')
    #  and substr(n.nspname, 1, 8) = 'pg_temp_'
    #  and t.relname = %s;
    #"""
    curs.execute(q, [tbl])
    tmp = curs.fetchall()
    return len(tmp) > 0

class TableCache:
    """Per-table data hander."""

    def __init__(self, tbl):
        """Init per-batch table data cache."""
        self.name = tbl
        self.ev_list = []
        self.pkey_map = {}
        self.pkey_list = []
        self.pkey_str = None
        self.col_list = None

        self.final_ins_list = []
        self.final_upd_list = []
        self.final_del_list = []

    def add_event(self, ev):
        """Store new event."""

        # op & data
        ev.op = ev.ev_type[0]
        ev.data = skytools.db_urldecode(ev.ev_data)

        # get pkey column names
        if self.pkey_str is None:
            if len(ev.ev_type) > 2:
                self.pkey_str = ev.ev_type.split(':')[1]
            else:
                self.pkey_str = ev.ev_extra2

            if self.pkey_str:
                self.pkey_list = self.pkey_str.split(',')

        # get pkey value
        if self.pkey_str:
            pk_data = []
            for k in self.pkey_list:
                pk_data.append(ev.data[k])
            ev.pk_data = tuple(pk_data)
        elif ev.op == 'I':
            # fake pkey, just to get them spread out
            ev.pk_data = ev.id
        else:
            raise Exception('non-pk tables not supported: %s' % self.name)

        # get full column list, detect added columns
        if not self.col_list:
            self.col_list = ev.data.keys()
        elif self.col_list != ev.data.keys():
            # ^ supposedly python guarantees same order in keys()

            # find new columns
            for c in ev.data.keys():
                if c not in self.col_list:
                    for oldev in self.ev_list:
                        oldev.data[c] = None
            self.col_list = ev.data.keys()

        # add to list
        self.ev_list.append(ev)

        # keep all versions of row data
        if ev.pk_data in self.pkey_map:
            self.pkey_map[ev.pk_data].append(ev)
        else:
            self.pkey_map[ev.pk_data] = [ev]

    def finish(self):
        """Got all data, prepare for insertion."""

        del_list = []
        ins_list = []
        upd_list = []
        for ev_list in self.pkey_map.values():
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

        # reorder cols
        new_list = self.pkey_list[:]
        for k in self.col_list:
            if k not in self.pkey_list:
                new_list.append(k)

        self.col_list = new_list
        self.final_ins_list = ins_list
        self.final_upd_list = upd_list
        self.final_del_list = del_list

class BulkLoader(pgq.SerialConsumer):
    def __init__(self, args):
        pgq.SerialConsumer.__init__(self, "bulk_loader", "src_db", "dst_db", args)

    def reload(self):
        pgq.SerialConsumer.reload(self)

        self.load_method = self.cf.getint("load_method", METH_CORRECT)
        if self.load_method not in (0,1,2):
            raise Exception("bad load_method")

        self.remap_tables = {}
        for map in self.cf.getlist("remap_tables", ''):
            tmp = map.split(':')
            tbl = tmp[0].strip()
            new = tmp[1].strip()
            self.remap_tables[tbl] = new

    def process_remote_batch(self, src_db, batch_id, ev_list, dst_db):
        """Content dispatcher."""

        # add events to per-table caches
        tables = {}
        for ev in ev_list:
            tbl = ev.extra1
            if not tbl in tables:
                tables[tbl] = TableCache(tbl)
            cache = tables[tbl]
            cache.add_event(ev)
            ev.tag_done()

        # then process them
        for tbl, cache in tables.items():
            cache.finish()
            self.process_one_table(dst_db, tbl, cache)

    def process_one_table(self, dst_db, tbl, cache):

        del_list = cache.final_del_list
        ins_list = cache.final_ins_list
        upd_list = cache.final_upd_list
        col_list = cache.col_list
        real_update_count = len(upd_list)

        self.log.debug("process_one_table: %s  (I/U/D = %d/%d/%d)" % (
                       tbl, len(ins_list), len(upd_list), len(del_list)))

        if tbl in self.remap_tables:
            old = tbl
            tbl = self.remap_tables[tbl]
            self.log.debug("Redirect %s to %s" % (old, tbl))

        # hack to unbroke stuff
        if self.load_method == METH_MERGED:
            upd_list += ins_list
            ins_list = []

        # check if interesting table
        curs = dst_db.cursor()
        if not skytools.exists_table(curs, tbl):
            self.log.warning("Ignoring events for table: %s" % tbl)
            return

        # fetch distribution fields
        dist_fields = find_dist_fields(curs, tbl)
        extra_fields = []
        for fld in dist_fields:
            if fld not in cache.pkey_list:
                extra_fields.append(fld)
        self.log.debug("PKey fields: %s  Extra fields: %s" % (
                       ",".join(cache.pkey_list), ",".join(extra_fields)))

        # create temp table
        temp = self.create_temp_table(curs, tbl)
        
        # where expr must have pkey and dist fields
        klist = []
        for pk in cache.pkey_list + extra_fields:
            exp = "%s.%s = %s.%s" % (quote_fqident(tbl), quote_ident(pk),
                                     quote_fqident(temp), quote_ident(pk))
            klist.append(exp)
        whe_expr = " and ".join(klist)

        # create del sql
        del_sql = "delete from only %s using %s where %s" % (
                  quote_fqident(tbl), quote_fqident(temp), whe_expr)

        # create update sql
        slist = []
        key_fields = cache.pkey_list + extra_fields
        for col in cache.col_list:
            if col not in key_fields:
                exp = "%s = %s.%s" % (quote_ident(col), quote_fqident(temp), quote_ident(col))
                slist.append(exp)
        upd_sql = "update only %s set %s from %s where %s" % (
                    quote_fqident(tbl), ", ".join(slist), quote_fqident(temp), whe_expr)

        # insert sql
        colstr = ",".join([quote_ident(c) for c in cache.col_list])
        ins_sql = "insert into %s (%s) select %s from %s" % (
                  quote_fqident(tbl), colstr, colstr, quote_fqident(temp))

        # process deleted rows
        if len(del_list) > 0:
            self.log.info("Deleting %d rows from %s" % (len(del_list), tbl))
            # delete old rows
            q = "truncate %s" % quote_fqident(temp)
            self.log.debug(q)
            curs.execute(q)
            # copy rows
            self.log.debug("COPY %d rows into %s" % (len(del_list), temp))
            skytools.magic_insert(curs, temp, del_list, col_list)
            # delete rows
            self.log.debug(del_sql)
            curs.execute(del_sql)
            self.log.debug("%s - %d" % (curs.statusmessage, curs.rowcount))
            self.log.debug(curs.statusmessage)
            if len(del_list) != curs.rowcount:
                self.log.warning("Delete mismatch: expected=%s updated=%d"
                        % (len(del_list), curs.rowcount))

        # process updated rows
        if len(upd_list) > 0:
            self.log.info("Updating %d rows in %s" % (len(upd_list), tbl))
            # delete old rows
            q = "truncate %s" % quote_fqident(temp)
            self.log.debug(q)
            curs.execute(q)
            # copy rows
            self.log.debug("COPY %d rows into %s" % (len(upd_list), temp))
            skytools.magic_insert(curs, temp, upd_list, col_list)
            if self.load_method == METH_CORRECT:
                # update main table
                self.log.debug(upd_sql)
                curs.execute(upd_sql)
                self.log.debug(curs.statusmessage)
                # check count
                if len(upd_list) != curs.rowcount:
                    self.log.warning("Update mismatch: expected=%s updated=%d"
                            % (len(upd_list), curs.rowcount))
            else:
                # delete from main table
                self.log.debug(del_sql)
                curs.execute(del_sql)
                self.log.debug(curs.statusmessage)
                # check count
                if real_update_count != curs.rowcount:
                    self.log.warning("Update mismatch: expected=%s deleted=%d"
                            % (real_update_count, curs.rowcount))
                # insert into main table
                if 0:
                    # does not work due bizgres bug
                    self.log.debug(ins_sql)
                    curs.execute(ins_sql)
                    self.log.debug(curs.statusmessage)
                else:
                    # copy again, into main table
                    self.log.debug("COPY %d rows into %s" % (len(upd_list), tbl))
                    skytools.magic_insert(curs, tbl, upd_list, col_list)

        # process new rows
        if len(ins_list) > 0:
            self.log.info("Inserting %d rows into %s" % (len(ins_list), tbl))
            skytools.magic_insert(curs, tbl, ins_list, col_list)

        # delete remaining rows
        if USE_LONGLIVED_TEMP_TABLES:
            q = "truncate %s" % quote_fqident(temp)
        else:
            # fscking problems with long-lived temp tables
            q = "drop table %s" % quote_fqident(temp)
        self.log.debug(q)
        curs.execute(q)

    def create_temp_table(self, curs, tbl):
        # create temp table for loading
        tempname = tbl.replace('.', '_') + "_loadertmp"

        # check if exists
        if USE_LONGLIVED_TEMP_TABLES:
            if exists_temp_table(curs, tempname):
                self.log.debug("Using existing temp table %s" % tempname)
                return tempname
    
        # bizgres crashes on delete rows
        arg = "on commit delete rows"
        arg = "on commit preserve rows"
        # create temp table for loading
        q = "create temp table %s (like %s) %s" % (
                quote_fqident(tempname), quote_fqident(tbl), arg)
        self.log.debug("Creating temp table: %s" % q)
        curs.execute(q)
        return tempname
        
if __name__ == '__main__':
    script = BulkLoader(sys.argv[1:])
    script.start()

