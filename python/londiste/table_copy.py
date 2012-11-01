#! /usr/bin/env python

"""Do a full table copy.

For internal usage.
"""

import sys, time, skytools

from londiste.util import find_copy_source
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
        if tblname not in self.table_map:
            self.log.warning('Table %s removed from replication', tblname)
            sys.exit(1)
        t = self.table_map[tblname]
        return t

    def do_copy(self, tbl_stat, src_db, dst_db):
        """Entry point into copying logic."""

        dst_db.commit()

        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        while 1:
            if tbl_stat.copy_role == 'wait-copy':
                self.log.info('waiting for first partition to initialize copy')
            elif tbl_stat.max_parallel_copies_reached():
                self.log.info('number of max parallel copies (%s) reached' %\
                                tbl_stat.max_parallel_copy)
            else:
                break
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

        src_real_table = pt.dest_table

        # 0 - dont touch
        # 1 - single tx
        # 2 - multi tx
        cmode = 1
        if tbl_stat.copy_role == 'lead':
            cmode = 2
        elif tbl_stat.copy_role:
            cmode = 0

        # We need to see COPY snapshot from txid_current_snapshot() later.
        oldiso = src_db.isolation_level
        src_db.set_isolation_level(skytools.I_REPEATABLE_READ)
        src_db.commit()

        self.sync_database_encodings(src_db, dst_db)

        self.log.info("Starting full copy of %s" % tbl_stat.name)

        # just in case, drop all fkeys (in case "replay" was skipped)
        # !! this may commit, so must be done before anything else !!
        if cmode > 0:
            self.drop_fkeys(dst_db, tbl_stat.dest_table)

        # now start ddl-dropping tx
        if cmode > 0:
            q = "lock table " + skytools.quote_fqident(tbl_stat.dest_table)
            dst_curs.execute(q)

        # find dst struct
        src_struct = TableStruct(src_curs, src_real_table)
        dst_struct = TableStruct(dst_curs, tbl_stat.dest_table)

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
                q += skytools.quote_fqident(tbl_stat.dest_table)
                dst_curs.execute(q)

            if cmode == 2 and tbl_stat.dropped_ddl is None:
                ddl = dst_struct.get_create_sql(objs)
                if ddl:
                    q = "select * from londiste.local_set_table_struct(%s, %s, %s)"
                    self.exec_cmd(dst_curs, q, [self.queue_name, tbl_stat.name, ddl])
                else:
                    ddl = None
                dst_db.commit()
                tbl_stat.dropped_ddl = ddl

        # do truncate & copy
        self.log.info("%s: start copy" % tbl_stat.name)
        p = tbl_stat.get_plugin()
        stats = p.real_copy(src_real_table, src_curs, dst_curs, common_cols)
        if stats:
            self.log.info("%s: copy finished: %d bytes, %d rows" % (
                          tbl_stat.name, stats[0], stats[1]))

        # get snapshot
        src_curs.execute("select txid_current_snapshot()")
        snapshot = src_curs.fetchone()[0]
        src_db.commit()

        # restore old behaviour
        src_db.set_isolation_level(oldiso)
        src_db.commit()

        tbl_stat.change_state(TABLE_CATCHING_UP)
        tbl_stat.change_snapshot(snapshot)
        self.save_table_state(dst_curs)

        # create previously dropped objects
        if cmode == 1:
            dst_struct.create(dst_curs, objs, log = self.log)
        elif cmode == 2:
            dst_db.commit()

            # start waiting for other copy processes to finish
            while tbl_stat.copy_role:
                self.log.info('waiting for other partitions to finish copy')
                time.sleep(10)
                tbl_stat = self.reload_table_stat(dst_curs, tbl_stat.name)
                dst_db.commit()

            if tbl_stat.dropped_ddl is not None:
                self.looping = 0
                for ddl in skytools.parse_statements(tbl_stat.dropped_ddl):
                    self.log.info(ddl)
                    dst_curs.execute(ddl)
                q = "select * from londiste.local_set_table_struct(%s, %s, NULL)"
                self.exec_cmd(dst_curs, q, [self.queue_name, tbl_stat.name])
                tbl_stat.dropped_ddl = None
                self.looping = 1
            dst_db.commit()

        # hack for copy-in-playback
        if not self.copy_thread:
            tbl_stat.change_state(TABLE_OK)
            self.save_table_state(dst_curs)
        dst_db.commit()

        # copy finished
        if tbl_stat.copy_role == 'wait-replay':
            return

        # if copy done, request immidiate tick from pgqadm,
        # to make state juggling faster.  on mostly idle db-s
        # each step may take tickers idle_timeout secs, which is pain.
        q = "select pgq.force_tick(%s)"
        src_curs.execute(q, [self.queue_name])
        src_db.commit()

    def work(self):
        if not self.reg_ok:
            # check if needed? (table, not existing reg)
            self.register_copy_consumer()
            self.reg_ok = True
        return Replicator.work(self)

    def register_copy_consumer(self):
        dst_db = self.get_database('db')
        dst_curs = dst_db.cursor()

        # fetch table attrs
        q = "select * from londiste.get_table_list(%s) where table_name = %s"
        dst_curs.execute(q, [ self.queue_name, self.copy_table_name ])
        rows = dst_curs.fetchall()
        attrs = {}
        if len(rows) > 0:
            v_attrs = rows[0]['table_attrs']
            if v_attrs:
                attrs = skytools.db_urldecode(v_attrs)

        # fetch parent consumer state
        q = "select * from pgq_node.get_consumer_state(%s, %s)"
        rows = self.exec_cmd(dst_db, q, [ self.queue_name, self.old_consumer_name ])
        state = rows[0]
        source_node = state['provider_node']
        source_location = state['provider_location']

        # do we have node here?
        if 'copy_node' in attrs:
            if attrs['copy_node'] == '?':
                source_node, source_location, wname = find_copy_source(self,
                        self.queue_name, self.copy_table_name, source_node, source_location)
            else:
                # take node from attrs
                source_node = attrs['copy_node']
                q = "select * from pgq_node.get_queue_locations(%s) where node_name = %s"
                dst_curs.execute(q, [ self.queue_name, source_node ])
                rows = dst_curs.fetchall()
                if len(rows):
                    source_location = rows[0]['node_location']

        self.log.info("Using '%s' as source node", source_node)
        self.register_consumer(source_location)

if __name__ == '__main__':
    script = CopyTable(sys.argv[1:])
    script.start()

