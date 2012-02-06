#! /usr/bin/env python

"""Do a full table copy.

For internal usage.
"""

import sys, os, skytools

from skytools.dbstruct import *
from playback import *

__all__ = ['CopyTable']

class CopyTable(Replicator):
    def __init__(self, args, copy_thread = 1):
        Replicator.__init__(self, args)

        if copy_thread:
            self.pidfile += ".copy"
            self.consumer_id += "_copy"
            self.copy_thread = 1

    def do_copy(self, tbl_stat):
        src_db = self.get_database('provider_db')
        dst_db = self.get_database('subscriber_db')

        # it should not matter to pgq
        src_db.commit()
        dst_db.commit()

        # we need to get the COPY snapshot later
        src_db.set_isolation_level(skytools.I_REPEATABLE_READ)
        src_db.commit()

        self.sync_database_encodings(src_db, dst_db)

        # initial sync copy
        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        self.log.info("Starting full copy of %s" % tbl_stat.name)

        # just in case, drop all fkeys (in case "replay" was skipped)
        # !! this may commit, so must be done before anything else !!
        self.drop_fkeys(dst_db, tbl_stat.name)

        # just in case, drop all triggers (in case "subscriber add" was skipped)
        q_triggers = "select londiste.subscriber_drop_all_table_triggers(%s)"
        dst_curs.execute(q_triggers, [tbl_stat.name])

        # find dst struct
        src_struct = TableStruct(src_curs, tbl_stat.name)
        dst_struct = TableStruct(dst_curs, tbl_stat.name)
        
        # check if columns match
        dlist = dst_struct.get_column_list()
        for c in src_struct.get_column_list():
            if c not in dlist:
                raise Exception('Column %s does not exist on dest side' % c)

        # drop unnecessary stuff
        objs = T_CONSTRAINT | T_INDEX | T_RULE | T_PARENT
        dst_struct.drop(dst_curs, objs, log = self.log)

        # do truncate & copy
        self.real_copy(src_curs, dst_curs, tbl_stat)

        # get snapshot
        src_curs.execute("select txid_current_snapshot()")
        snapshot = src_curs.fetchone()[0]
        src_db.commit()

        # restore READ COMMITTED behaviour
        src_db.set_isolation_level(skytools.I_READ_COMMITTED)
        src_db.commit()

        # create previously dropped objects
        dst_struct.create(dst_curs, objs, log = self.log)
        dst_db.commit()

        # set state
        if self.copy_thread:
            tbl_stat.change_state(TABLE_CATCHING_UP)
        else:
            tbl_stat.change_state(TABLE_OK)
        tbl_stat.change_snapshot(snapshot)
        self.save_table_state(dst_curs)
        dst_db.commit()

        self.log.debug("%s: ANALYZE" % tbl_stat.name)
        dst_curs.execute("analyze " + skytools.quote_fqident(tbl_stat.name))
        dst_db.commit()

        # if copy done, request immidiate tick from pgqadm,
        # to make state juggling faster.  on mostly idle db-s
        # each step may take tickers idle_timeout secs, which is pain.
        q = "select pgq.force_tick(%s)"
        src_curs.execute(q, [self.pgq_queue_name])
        src_db.commit()

    def real_copy(self, srccurs, dstcurs, tbl_stat):
        "Main copy logic."

        tablename = tbl_stat.name
        # drop data
        if tbl_stat.skip_truncate:
            self.log.info("%s: skipping truncate" % tablename)
        else:
            self.log.info("%s: truncating" % tablename)
            # truncate behaviour changed in 8.4
            dstcurs.execute("show server_version_num")
            pgver = int(dstcurs.fetchone()[0])
            if pgver >= 80400:
                dstcurs.execute("truncate only " + skytools.quote_fqident(tablename))
            else:
                dstcurs.execute("truncate " + skytools.quote_fqident(tablename))

        # do copy
        self.log.info("%s: start copy" % tablename)
        col_list = skytools.get_table_columns(srccurs, tablename)
        stats = skytools.full_copy(tablename, srccurs, dstcurs, col_list)
        if stats:
            self.log.info("%s: copy finished: %d bytes, %d rows" % (
                          tablename, stats[0], stats[1]))

if __name__ == '__main__':
    script = CopyTable(sys.argv[1:])
    script.start()

