#! /usr/bin/env python

"""Bulkloader for slow databases (Bizgres).

Idea is following:
    - Script reads from queue a batch of urlencoded row changes.
      Inserts/updates/deletes, maybe many per one row.
    - It changes them to minimal amount of DELETE commands
      followed by big COPY of new data.
    - One side-effect is that total order of how rows appear
      changes, but per-row changes will be kept in order.

The speedup from the COPY will happen only if the batches are
large enough.  So the ticks should happen only after couple
of minutes.

"""

import sys, os, pgq, skytools

def mk_delete_sql(tbl, key_list, data):
    """ generate delete command """
    whe_list = []
    for k in key_list:
        whe_list.append("%s = %s" % (k, skytools.quote_literal(data[k])))
    whe_str = " and ".join(whe_list)
    return "delete from %s where %s;" % (tbl, whe_str)

class TableCache(object):
    """Per-table data hander."""

    def __init__(self, tbl):
        """Init per-batch table data cache."""
        self.name = tbl
        self.ev_list = []
        self.pkey_map = {}
        self.pkey_list = []
        self.pkey_str = None
        self.col_list = None

    def add_event(self, ev):
        """Store new event."""

        # op & data
        ev.op = ev.type[0]
        ev.row = skytools.db_urldecode(ev.data)

        # get pkey column names
        if self.pkey_str is None:
            self.pkey_str = ev.type.split(':')[1]
            if self.pkey_str:
                self.pkey_list = self.pkey_str.split(',')

        # get pkey value
        if self.pkey_str:
            pk_data = []
            for k in self.pkey_list:
                pk_data.append(ev.row[k])
            ev.pk_data = tuple(pk_data)
        elif ev.op == 'I':
            # fake pkey, just to get them spread out
            ev.pk_data = ev.id
        else:
            raise Exception('non-pk tables not supported: %s' % ev.extra1)

        # get full column list, detect added columns
        if not self.col_list:
            self.col_list = ev.row.keys()
        elif self.col_list != ev.row.keys():
            # ^ supposedly python guarantees same order in keys()

            # find new columns
            for c in ev.row.keys():
                if c not in self.col_list:
                    for oldev in self.ev_list:
                        oldev.row[c] = None
            self.col_list = ev.row.keys()

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
        copy_list = []
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
            if exists_before:
                del_list.append(mk_delete_sql(self.name, self.pkey_list, ev.row))
            if exists_after:
                copy_list.append(ev.row)

        # reorder cols
        new_list = self.pkey_list[:]
        for k in self.col_list:
            if k not in self.pkey_list:
                new_list.append(k)

        return del_list, new_list, copy_list
            
class BulkLoader(pgq.SerialConsumer):
    def __init__(self, args):
        pgq.SerialConsumer.__init__(self, "bulk_loader", "src_db", "dst_db", args)

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
            self.process_one_table(dst_db, tbl, cache)

    def process_one_table(self, dst_db, tbl, cache):
        self.log.debug("process_one_table: %s" % tbl)
        del_list, col_list, copy_list = cache.finish()
        curs = dst_db.cursor()

        if not skytools.exists_table(curs, tbl):
            self.log.warning("Ignoring events for table: %s" % tbl)
            return

        if len(del_list) > 0:
            self.log.info("Deleting %d rows from %s" % (len(del_list), tbl))
                    
            q = " ".join(del_list)
            self.log.debug(q)
            curs.execute(q)

        if len(copy_list) > 0:
            self.log.info("Copying %d rows into %s" % (len(copy_list), tbl))
            self.log.debug("COPY %s (%s)" % (tbl, ','.join(col_list)))
            skytools.magic_insert(curs, tbl, copy_list, col_list)

if __name__ == '__main__':
    script = BulkLoader(sys.argv[1:])
    script.start()

