#! /usr/bin/env python

# it accepts urlencoded rows for multiple tables from queue
# and insert them into actual tables, with partitioning on tick time

import sys, os, pgq, skytools

DEF_CREATE = """
create table _DEST_TABLE (like _PARENT);
alter table only _DEST_TABLE add primary key (_PKEY);
"""

class CubeDispatcher(pgq.SerialConsumer):
    def __init__(self, args):
        pgq.SerialConsumer.__init__(self, "cube_dispatcher", "src_db", "dst_db", args)

        self.dateformat = self.cf.get('dateformat', 'YYYY_MM_DD')

        self.part_template = self.cf.get('part_template', DEF_CREATE)

        mode = self.cf.get('mode', 'keep_latest')
        if mode == 'keep_latest':
            self.keep_latest = 1
        elif mode == 'keep_all':
            self.keep_latest = 0
        else:
            self.log.fatal('wrong mode setting')
            sys.exit(1)

    def get_part_date(self, batch_id):
        if not self.dateformat:
            return None

        # fetch and format batch date
        src_db = self.get_database('src_db')
        curs = src_db.cursor()
        q = 'select to_char(batch_end, %s) from pgq.get_batch_info(%s)'
        curs.execute(q, [self.dateformat, batch_id])
        src_db.commit()
        return curs.fetchone()[0]

    def process_remote_batch(self, src_db, batch_id, ev_list, dst_db):
        
        # actual processing
        date_str = self.get_part_date(batch_id)
        self.dispatch(dst_db, ev_list, self.get_part_date(batch_id))

    def dispatch(self, dst_db, ev_list, date_str):
        """Actual event processing."""

        # get tables and sql
        tables = {}
        sql_list = []
        for ev in ev_list:
            if date_str:
                tbl = "%s_%s" % (ev.extra1, date_str)
            else:
                tbl = ev.extra1

            sql = self.make_sql(tbl, ev)
            sql_list.append(sql)

            if not tbl in tables:
                tables[tbl] = self.get_table_info(ev, tbl)

            ev.tag_done()

        # create tables if needed
        self.check_tables(dst_db, tables)

        # insert into data tables
        curs = dst_db.cursor()
        block = []
        for sql in sql_list:
            self.log.debug(sql)
            block.append(sql)
            if len(block) > 100:
                curs.execute("\n".join(block))
                block = []
        if len(block) > 0:
            curs.execute("\n".join(block))
    
    def get_table_info(self, ev, tbl):
        klist = [skytools.quote_ident(k) for k in ev.key_list.split(',')]
        inf = {
            'parent': ev.extra1,
            'table': tbl,
            'key_list': ",".join(klist),
        }
        return inf

    def make_sql(self, tbl, ev):
        """Return SQL statement(s) for that event."""
        
        # parse data
        data = skytools.db_urldecode(ev.data)
            
        # parse tbl info
        if ev.type.find(':') > 0:
            op, keys = ev.type.split(':')
        else:
            op = ev.type
            keys = ev.extra2
        ev.key_list = keys
        key_list = keys.split(',')
        if self.keep_latest and len(key_list) == 0:
            raise Exception('No pkey on table %s' % tbl)

        # generate sql
        if op in ('I', 'U'):
            if self.keep_latest:
                sql = "%s %s" % (self.mk_delete_sql(tbl, key_list, data),
                                 self.mk_insert_sql(tbl, key_list, data))
            else:
                sql = self.mk_insert_sql(tbl, key_list, data)
        elif op == "D":
            if not self.keep_latest:
                raise Exception('Delete op not supported if mode=keep_all')

            sql = self.mk_delete_sql(tbl, key_list, data)
        else:
            raise Exception('Unknown row op: %s' % op)
        return sql
        
    def mk_delete_sql(self, tbl, key_list, data):
        # generate delete command
        whe_list = []
        for k in key_list:
            whe_list.append("%s = %s" % (skytools.quote_ident(k), skytools.quote_literal(data[k])))
        whe_str = " and ".join(whe_list)
        return "delete from %s where %s;" % (skytools.quote_fqident(tbl), whe_str)
            
    def mk_insert_sql(self, tbl, key_list, data):
        # generate insert command
        col_list = []
        val_list = []
        for c, v in data.items():
            col_list.append(skytools.quote_ident(c))
            val_list.append(skytools.quote_literal(v))
        col_str = ",".join(col_list)
        val_str = ",".join(val_list)
        return "insert into %s (%s) values (%s);" % (
                        skytools.quote_fqident(tbl), col_str, val_str)

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
        for tbl, inf in tables.items():
            if skytools.exists_table(dcur, tbl):
                continue

            sql = self.part_template
            sql = sql.replace('_DEST_TABLE', skytools.quote_fqident(inf['table']))
            sql = sql.replace('_PARENT', skytools.quote_fqident(inf['parent']))
            sql = sql.replace('_PKEY', inf['key_list'])
            # be similar to table_dispatcher
            schema_table = inf['table'].replace(".", "__")
            sql = sql.replace('_SCHEMA_TABLE', skytools.quote_ident(schema_table))

            dcur.execute(sql)
            dcon.commit()
            self.log.info('%s: Created table %s' % (self.job_name, tbl))

if __name__ == '__main__':
    script = CubeDispatcher(sys.argv[1:])
    script.start()

