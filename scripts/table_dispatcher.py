#! /usr/bin/env python

# it loads urlencoded rows for one trable from queue and inserts
# them into actual tables, with optional partitioning

import sys, os, pgq, skytools

DEST_TABLE = "_DEST_TABLE"
SCHEMA_TABLE = "_SCHEMA_TABLE"

class TableDispatcher(pgq.SerialConsumer):
    def __init__(self, args):
        pgq.SerialConsumer.__init__(self, "table_dispatcher", "src_db", "dst_db", args)

        self.part_template = self.cf.get("part_template", '')
        self.dest_table = self.cf.get("dest_table")
        self.part_field = self.cf.get("part_field", '')
        self.part_method = self.cf.get("part_method", 'daily')
        if self.part_method not in ('daily', 'monthly'):
            raise Exception('bad part_method')

        if self.cf.get("fields", "*") == "*":
            self.field_map = None
        else:
            self.field_map = {}
            for fval in self.cf.getlist('fields'):
                tmp = fval.split(':')
                if len(tmp) == 1:
                    self.field_map[tmp[0]] = tmp[0]
                else:
                    self.field_map[tmp[0]] = tmp[1]

    def process_remote_batch(self, src_db, batch_id, ev_list, dst_db):
        # actual processing
        self.dispatch(dst_db, ev_list)

    def dispatch(self, dst_db, ev_list):
        """Generic dispatcher."""

        # load data
        tables = {}
        for ev in ev_list:
            row = skytools.db_urldecode(ev.data)

            # guess dest table
            if self.part_field:
                if self.part_field == "_EVTIME":
                    partval = str(ev.creation_date)
                else:
                    partval = str(row[self.part_field])
                partval = partval.split(' ')[0]
                date = partval.split('-')
                if self.part_method == 'monthly':
                    date = date[:2]
                suffix = '_'.join(date)
                tbl = "%s_%s" % (self.dest_table, suffix)
            else:
                tbl = self.dest_table

            # map fields
            if self.field_map is None:
                dstrow = row
            else:
                dstrow = {}
                for k, v in self.field_map.items():
                    dstrow[v] = row[k]

            # add row into table
            if not tbl in tables:
                tables[tbl] = [dstrow]
            else:
                tables[tbl].append(dstrow)

            ev.tag_done()

        # create tables if needed
        self.check_tables(dst_db, tables)

        # insert into data tables
        curs = dst_db.cursor()
        for tbl, tbl_rows in tables.items():
            skytools.magic_insert(curs, tbl, tbl_rows)

    def check_tables(self, dcon, tables):
        """Checks that tables needed for copy are there. If not
        then creates them.

        Used by other procedures to ensure that table is there
        before they start inserting.

        The commits should not be dangerous, as we haven't done anything
        with cdr's yet, so they should still be in one TX.

        Although it would be nicer to have a lock for table creation.
        """

        dcur = dcon.cursor()
        exist_map = {}
        for tbl in tables.keys():
            if not skytools.exists_table(dcur, tbl):
                if not self.part_template:
                    raise Exception('Dest table does not exists and no way to create it.')

                sql = self.part_template
                sql = sql.replace(DEST_TABLE, skytools.quote_fqident(tbl))

                # we do this to make sure that constraints for 
                # tables who contain a schema will still work
                schema_table = tbl.replace(".", "__")
                sql = sql.replace(SCHEMA_TABLE, skytools.quote_ident(schema_table))

                dcur.execute(sql)
                dcon.commit()
                self.log.info('%s: Created table %s' % (self.job_name, tbl))

if __name__ == '__main__':
    script = TableDispatcher(sys.argv[1:])
    script.start()

