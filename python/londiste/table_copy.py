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

        if not copy_thread:
            raise Exception("Combined copy not supported")

        if len(self.args):
            print "londiste copy requires table name"
        self.copy_table_name = self.args[2]

        self.pidfile += ".copy.%s" % self.copy_table_name
        self.consumer_name += "_copy_%s" % self.copy_table_name
        self.copy_thread = 1
        self.main_worker = False

    def do_copy(self, tbl_stat, src_db, dst_db):

        dst_db.commit()

        while 1:
            pmap = self.get_state_map(src_db.cursor())
            src_db.commit()
            if tbl_stat.name not in pmap:
                raise Excpetion("table %s not available on provider" % tbl_stat.name)
            pt = pmap[tbl_stat.name]
            if pt.state == TABLE_OK:
                break
            
            self.log.warning("table %s not in sync yet on provider, waiting" % tbl_stat.name)
            time.sleep(10)


        # change to SERIALIZABLE isolation level
        src_db.set_isolation_level(skytools.I_SERIALIZABLE)
        src_db.commit()

        self.sync_database_encodings(src_db, dst_db)

        # initial sync copy
        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()

        self.log.info("Starting full copy of %s" % tbl_stat.name)

        # just in case, drop all fkeys (in case "replay" was skipped)
        # !! this may commit, so must be done before anything else !!
        self.drop_fkeys(dst_db, tbl_stat.name)

        # drop own triggers
        q_node_trg = "select * from londiste.node_disable_triggers(%s, %s)"
        dst_curs.execute(q_node_trg, [self.set_name, tbl_stat.name])

        # drop rest of the triggers
        q_triggers = "select londiste.drop_all_table_triggers(%s)"
        dst_curs.execute(q_triggers, [tbl_stat.name])

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
        objs = T_CONSTRAINT | T_INDEX | T_RULE
        dst_struct.drop(dst_curs, objs, log = self.log)

        # do truncate & copy
        self.real_copy(src_curs, dst_curs, tbl_stat, common_cols)

        # get snapshot
        src_curs.execute("select txid_current_snapshot()")
        snapshot = src_curs.fetchone()[0]
        src_db.commit()

        # restore READ COMMITTED behaviour
        src_db.set_isolation_level(1)
        src_db.commit()

        # restore own triggers
        q_node_trg = "select * from londiste.node_refresh_triggers(%s, %s)"
        dst_curs.execute(q_node_trg, [self.set_name, tbl_stat.name])

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

    def real_copy(self, srccurs, dstcurs, tbl_stat, col_list):
        "Main copy logic."

        tablename = tbl_stat.name
        # drop data
        if tbl_stat.skip_truncate:
            self.log.info("%s: skipping truncate" % tablename)
        else:
            self.log.info("%s: truncating" % tablename)
            dstcurs.execute("truncate " + tablename)

        # do copy
        self.log.info("%s: start copy" % tablename)
        stats = skytools.full_copy(tablename, srccurs, dstcurs, col_list)
        if stats:
            self.log.info("%s: copy finished: %d bytes, %d rows" % (
                          tablename, stats[0], stats[1]))

if __name__ == '__main__':
    script = CopyTable(sys.argv[1:])
    script.start()

