#! /usr/bin/env python

"""Do a full table copy.

For internal usage.
"""

import sys, time, skytools

from skytools.dbstruct import *
from londiste.playback import *

__all__ = ['CopyTable']

class CopyTable(Replicator):
    """Table copy thread implementation."""

    reg_ok = False

    def __init__(self, args, copy_thread = 1):
        """Initializer.  copy_thread arg shows if the copy process is separate
        from main Playback thread or not.  copy_thread=0 means copying happens
        in same process.
        """

        Replicator.__init__(self, args)

        if not copy_thread:
            raise Exception("Combined copy not supported")

        if len(self.args) != 3:
            self.log.error("londiste copy requires table name")
            sys.exit(1)
        self.copy_table_name = self.args[2]

        sfx = self.get_copy_suffix(self.copy_table_name)
        self.old_consumer_name = self.consumer_name
        self.pidfile += sfx
        self.consumer_name += sfx
        self.copy_thread = 1
        self.main_worker = False

    def get_copy_suffix(self, tblname):
        return ".copy.%s" % tblname

    def reload_table_stat(self, dst_curs, tblname):
        self.load_table_state(dst_curs)
        t = self.table_map[tblname]
        return t

    def do_copy(self, tbl_stat, src_db, dst_db):
        """Entry point into copying logic."""

        dst_db.commit()

        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        while tbl_stat.copy_role == 'wait-copy':
            self.log.info('waiting for first partition to initialize copy')
            time.sleep(10)
            tbl_stat = self.reload_table_stat(dst_curs, tbl_stat.name)
            dst_db.commit()

        while 1:
            pmap = self.get_state_map(src_db.cursor())
            src_db.commit()
            if tbl_stat.name not in pmap:
                raise Exception("table %s not available on provider" % tbl_stat.name)
            pt = pmap[tbl_stat.name]
            if pt.state == TABLE_OK:
                break
            
            self.log.warning("table %s not in sync yet on provider, waiting" % tbl_stat.name)
            time.sleep(10)

        # 0 - dont touch
        # 1 - single tx
        # 2 - multi tx
        cmode = 1
        if tbl_stat.copy_role == 'lead':
            cmode = 2
        elif tbl_stat.copy_role:
            cmode = 0

        # change to SERIALIZABLE isolation level
        src_db.set_isolation_level(skytools.I_SERIALIZABLE)
        src_db.commit()

        self.sync_database_encodings(src_db, dst_db)

        self.log.info("Starting full copy of %s" % tbl_stat.name)

        # just in case, drop all fkeys (in case "replay" was skipped)
        # !! this may commit, so must be done before anything else !!
        self.drop_fkeys(dst_db, tbl_stat.name)

        # find dst struct
        src_struct = TableStruct(src_curs, tbl_stat.name)
        dst_struct = TableStruct(dst_curs, tbl_stat.name)
        
        # take common columns, warn on missing ones
        dlist = dst_struct.get_column_list()
        slist = src_struct.get_column_list()
        common_cols = []
        for c in slist:
            if c not in dlist:
                self.log.warning("Table %s column %s does not exist on subscriber"
                                 % (tbl_stat.name, c))
            else:
                common_cols.append(c)
        for c in dlist:
            if c not in slist:
                self.log.warning("Table %s column %s does not exist on provider"
                                 % (tbl_stat.name, c))

        # drop unnecessary stuff
        if cmode > 0:
            objs = T_CONSTRAINT | T_INDEX | T_RULE | T_PARENT # | T_TRIGGER
            dst_struct.drop(dst_curs, objs, log = self.log)

            # drop data
            if tbl_stat.table_attrs.get('skip_truncate'):
                self.log.info("%s: skipping truncate" % tbl_stat.name)
            else:
                self.log.info("%s: truncating" % tbl_stat.name)
                q = "truncate "
                if dst_db.server_version >= 80400:
                    q += "only "
                q += skytools.quote_fqident(tbl_stat.name)
                dst_curs.execute(q)

            if cmode == 2 and tbl_stat.dropped_ddl is None:
                ddl = dst_struct.get_create_sql(objs)
                q = "select * from londiste.local_set_table_struct(%s, %s, %s)"
                self.exec_cmd(dst_curs, q, [self.queue_name, tbl_stat.name, ddl])
                dst_db.commit()
                tbl_stat.dropped_ddl = ddl

        # do truncate & copy
        self.real_copy(src_curs, dst_curs, tbl_stat, common_cols)

        # get snapshot
        src_curs.execute("select txid_current_snapshot()")
        snapshot = src_curs.fetchone()[0]
        src_db.commit()

        # restore READ COMMITTED behaviour
        src_db.set_isolation_level(1)
        src_db.commit()

        # create previously dropped objects
        if cmode == 1:
            dst_struct.create(dst_curs, objs, log = self.log)
        elif cmode == 2:
            dst_db.commit()
            while tbl_stat.copy_role == 'lead':
                self.log.info('waiting for other partitions to finish copy')
                time.sleep(10)
                tbl_stat = self.reload_table_stat(dst_curs, tbl_stat.name)
                dst_db.commit()

            if tbl_stat.dropped_ddl is not None:
                for ddl in skytools.parse_statements(tbl_stat.dropped_ddl):
                    self.log.info(ddl)
                    dst_curs.execute(ddl)
                q = "select * from londiste.local_set_table_struct(%s, %s, NULL)"
                self.exec_cmd(dst_curs, q, [self.queue_name, tbl_stat.name])
                tbl_stat.dropped_ddl = None
            dst_db.commit()

        # set state
        if self.copy_thread:
            tbl_stat.change_state(TABLE_CATCHING_UP)
        else:
            tbl_stat.change_state(TABLE_OK)
        tbl_stat.change_snapshot(snapshot)
        self.save_table_state(dst_curs)
        dst_db.commit()

        # copy finished
        if tbl_stat.copy_role == 'wait-replay':
            return

        # analyze
        self.log.info("%s: analyze" % tbl_stat.name)
        dst_curs.execute("analyze " + skytools.quote_fqident(tbl_stat.name))
        dst_db.commit()

        # if copy done, request immidiate tick from pgqadm,
        # to make state juggling faster.  on mostly idle db-s
        # each step may take tickers idle_timeout secs, which is pain.
        q = "select pgq.force_tick(%s)"
        src_curs.execute(q, [self.queue_name])
        src_db.commit()

    def real_copy(self, srccurs, dstcurs, tbl_stat, col_list):
        "Actual copy."

        tablename = tbl_stat.name
        # do copy
        self.log.info("%s: start copy" % tablename)
        p = tbl_stat.get_plugin()
        cond_list = []
        cond = tbl_stat.table_attrs.get('copy_condition')
        if cond:
            cond_list.append(cond)
        p.prepare_copy(cond_list, dstcurs)
        w_cond = ' and '.join(cond_list)
        stats = skytools.full_copy(tablename, srccurs, dstcurs, col_list, w_cond)
        if stats:
            self.log.info("%s: copy finished: %d bytes, %d rows" % (
                          tablename, stats[0], stats[1]))

    def work(self):
        if not self.reg_ok:
            # check if needed? (table, not existing reg)
            self.register_copy_consumer()
            self.reg_ok = True
        return Replicator.work(self)

    def register_copy_consumer(self):
        # fetch parent consumer state
        dst_db = self.get_database('db')
        q = "select * from pgq_node.get_consumer_state(%s, %s)"
        rows = self.exec_cmd(dst_db, q, [ self.queue_name, self.old_consumer_name ])
        state = rows[0]
        loc = state['provider_location']

        self.register_consumer(loc)

if __name__ == '__main__':
    script = CopyTable(sys.argv[1:])
    script.start()

