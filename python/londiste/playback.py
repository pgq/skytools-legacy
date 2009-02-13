#! /usr/bin/env python

"""Basic replication core."""

import sys, os, time
import skytools

from pgq.cascade.worker import CascadedWorker

__all__ = ['Replicator', 'TableState',
    'TABLE_MISSING', 'TABLE_IN_COPY', 'TABLE_CATCHING_UP',
    'TABLE_WANNA_SYNC', 'TABLE_DO_SYNC', 'TABLE_OK']

# state                 # owner - who is allowed to change
TABLE_MISSING      = 0  # main
TABLE_IN_COPY      = 1  # copy
TABLE_CATCHING_UP  = 2  # copy
TABLE_WANNA_SYNC   = 3  # main
TABLE_DO_SYNC      = 4  # copy
TABLE_OK           = 5  # setup

SYNC_OK   = 0  # continue with batch
SYNC_LOOP = 1  # sleep, try again
SYNC_EXIT = 2  # nothing to do, exit skript

class Counter(object):
    """Counts table statuses."""

    missing = 0
    copy = 0
    catching_up = 0
    wanna_sync = 0
    do_sync = 0
    ok = 0

    def __init__(self, tables):
        """Counts and sanity checks."""
        for t in tables:
            if t.state == TABLE_MISSING:
                self.missing += 1
            elif t.state == TABLE_IN_COPY:
                self.copy += 1
            elif t.state == TABLE_CATCHING_UP:
                self.catching_up += 1
            elif t.state == TABLE_WANNA_SYNC:
                self.wanna_sync += 1
            elif t.state == TABLE_DO_SYNC:
                self.do_sync += 1
            elif t.state == TABLE_OK:
                self.ok += 1

    def get_copy_count(self):
        return self.copy + self.catching_up + self.wanna_sync + self.do_sync

class TableState(object):
    """Keeps state about one table."""
    def __init__(self, name, log):
        """Init TableState for one table."""
        self.name = name
        self.log = log
        # same as forget:
        self.state = TABLE_MISSING
        self.last_snapshot_tick = None
        self.str_snapshot = None
        self.from_snapshot = None
        self.sync_tick_id = None
        self.ok_batch_count = 0
        self.last_tick = 0
        self.skip_truncate = False
        self.copy_role = None
        self.dropped_ddl = None
        # except this
        self.changed = 0

    def forget(self):
        """Reset all info."""
        self.state = TABLE_MISSING
        self.last_snapshot_tick = None
        self.str_snapshot = None
        self.from_snapshot = None
        self.sync_tick_id = None
        self.ok_batch_count = 0
        self.last_tick = 0
        self.skip_truncate = False
        self.changed = 1

    def change_snapshot(self, str_snapshot, tag_changed = 1):
        """Set snapshot."""
        if self.str_snapshot == str_snapshot:
            return
        self.log.debug("%s: change_snapshot to %s" % (self.name, str_snapshot))
        self.str_snapshot = str_snapshot
        if str_snapshot:
            self.from_snapshot = skytools.Snapshot(str_snapshot)
        else:
            self.from_snapshot = None

        if tag_changed:
            self.ok_batch_count = 0
            self.last_tick = None
            self.changed = 1

    def change_state(self, state, tick_id = None):
        """Set state."""
        if self.state == state and self.sync_tick_id == tick_id:
            return
        self.state = state
        self.sync_tick_id = tick_id
        self.changed = 1
        self.log.debug("%s: change_state to %s" % (self.name,
                                    self.render_state()))

    def render_state(self):
        """Make a string to be stored in db."""

        if self.state == TABLE_MISSING:
            return None
        elif self.state == TABLE_IN_COPY:
            return 'in-copy'
        elif self.state == TABLE_CATCHING_UP:
            return 'catching-up'
        elif self.state == TABLE_WANNA_SYNC:
            return 'wanna-sync:%d' % self.sync_tick_id
        elif self.state == TABLE_DO_SYNC:
            return 'do-sync:%d' % self.sync_tick_id
        elif self.state == TABLE_OK:
            return 'ok'

    def parse_state(self, merge_state):
        """Read state from string."""

        state = -1
        if merge_state == None:
            state = TABLE_MISSING
        elif merge_state == "in-copy":
            state = TABLE_IN_COPY
        elif merge_state == "catching-up":
            state = TABLE_CATCHING_UP
        elif merge_state == "ok":
            state = TABLE_OK
        elif merge_state == "?":
            state = TABLE_OK
        else:
            tmp = merge_state.split(':')
            if len(tmp) == 2:
                self.sync_tick_id = int(tmp[1])
                if tmp[0] == 'wanna-sync':
                    state = TABLE_WANNA_SYNC
                elif tmp[0] == 'do-sync':
                    state = TABLE_DO_SYNC

        if state < 0:
            raise Exception("Bad table state: %s" % merge_state)

        return state

    def loaded_state(self, row):
        """Update object with info from db."""

        self.log.debug("loaded_state: %s: %s / %s" % (
                       self.name, row['merge_state'], row['custom_snapshot']))
        self.change_snapshot(row['custom_snapshot'], 0)
        self.state = self.parse_state(row['merge_state'])
        self.changed = 0
        self.skip_truncate = row['skip_truncate']
        self.copy_role = row['copy_role']
        self.dropped_ddl = row['dropped_ddl']
        if row['merge_state'] == "?":
            self.changed = 1

    def interesting(self, ev, tick_id, copy_thread):
        """Check if table wants this event."""

        if copy_thread:
            if self.state not in (TABLE_CATCHING_UP, TABLE_DO_SYNC):
                return False
        else:
            if self.state != TABLE_OK:
                return False

        # if no snapshot tracking, then accept always
        if not self.from_snapshot:
            return True

        # uninteresting?
        if self.from_snapshot.contains(ev.txid):
            return False

        # after couple interesting batches there no need to check snapshot
        # as there can be only one partially interesting batch
        if tick_id != self.last_tick:
            self.last_tick = tick_id
            self.ok_batch_count += 1

            # disable batch tracking
            if self.ok_batch_count > 3:
                self.change_snapshot(None)
        return True

    def gc_snapshot(self, copy_thread, prev_tick, cur_tick, no_lag):
        """Remove attached snapshot if possible.
        
        If the event processing is in current moment. the snapshot
        is not needed beyond next batch.

        The logic is needed for mostly unchanging tables,
        where the .ok_batch_count check in .interesting()
        method can take a lot of time.
        """

        # check if gc is needed
        if self.str_snapshot is None:
            return

        # check if allowed to modify
        if copy_thread:
            if self.state != TABLE_CATCHING_UP:
                return
        else:
            if self.state != TABLE_OK:
                return False

        # aquire last tick
        if not self.last_snapshot_tick:
            if no_lag:
                self.last_snapshot_tick = cur_tick
            return

        # reset snapshot if not needed anymore
        if self.last_snapshot_tick < prev_tick:
            self.change_snapshot(None)

class Replicator(CascadedWorker):
    """Replication core."""

    sql_command = {
        'I': "insert into %s %s;",
        'U': "update only %s set %s;",
        'D': "delete from only %s where %s;",
    }

    # batch info
    cur_tick = 0
    prev_tick = 0
    copy_table_name = None # filled by Copytable()
    sql_list = []

    def __init__(self, args):
        """Replication init."""
        CascadedWorker.__init__(self, 'londiste', 'db', args)

        self.table_list = []
        self.table_map = {}

        self.copy_thread = 0
        self.set_name = self.queue_name

        self.parallel_copies = self.cf.getint('parallel_copies', 1)
        if self.parallel_copies < 1:
            raise Exception('Bad value for parallel_copies: %d' % self.parallel_copies)

    def connection_setup(self, dbname, db):
        if dbname == 'db':
            curs = db.cursor()
            curs.execute("set session_replication_role = 'replica'")
            db.commit()

    def process_remote_batch(self, src_db, tick_id, ev_list, dst_db):
        "All work for a batch.  Entry point from SetConsumer."

        # this part can play freely with transactions

        self.sync_database_encodings(src_db, dst_db)
        
        self.cur_tick = self._batch_info['tick_id']
        self.prev_tick = self._batch_info['prev_tick_id']

        dst_curs = dst_db.cursor()
        self.load_table_state(dst_curs)
        self.sync_tables(src_db, dst_db)

        self.copy_snapshot_cleanup(dst_db)

        # only main thread is allowed to restore fkeys
        if not self.copy_thread:
            self.restore_fkeys(dst_db)

        # now the actual event processing happens.
        # they must be done all in one tx in dst side
        # and the transaction must be kept open so that
        # the cascade-consumer can save last tick and commit.

        self.sql_list = []
        CascadedWorker.process_remote_batch(self, src_db, tick_id, ev_list, dst_db)
        self.flush_sql(dst_curs)

        # finalize table changes
        self.save_table_state(dst_curs)

    def sync_tables(self, src_db, dst_db):
        """Table sync loop.
        
        Calls appropriate handles, which is expected to
        return one of SYNC_* constants."""

        self.log.debug('Sync tables')
        while 1:
            cnt = Counter(self.table_list)
            if self.copy_thread:
                res = self.sync_from_copy_thread(cnt, src_db, dst_db)
            else:
                res = self.sync_from_main_thread(cnt, src_db, dst_db)

            if res == SYNC_EXIT:
                self.log.debug('Sync tables: exit')
                if not self.copy_thread:
                    self.unregister_consumer()
                src_db.commit()
                sys.exit(0)
            elif res == SYNC_OK:
                return
            elif res != SYNC_LOOP:
                raise Exception('Program error')

            self.log.debug('Sync tables: sleeping')
            time.sleep(3)
            dst_db.commit()
            self.load_table_state(dst_db.cursor())
            dst_db.commit()
    
    def sync_from_main_thread(self, cnt, src_db, dst_db):
        "Main thread sync logic."

        # This operates on all table, any amount can be in any state

        ret = SYNC_OK
        
        if cnt.do_sync:
            # wait for copy thread to catch up
            ret = SYNC_LOOP
        
        for t in self.get_tables_in_state(TABLE_WANNA_SYNC):
            # copy thread wants sync, if not behind, do it
            if self.cur_tick >= t.sync_tick_id:
                self.change_table_state(dst_db, t, TABLE_DO_SYNC, self.cur_tick)
                ret = SYNC_LOOP

        npossible = self.parallel_copies - cnt.get_copy_count()
        if cnt.missing and npossible > 0:
            pmap = self.get_state_map(src_db.cursor())
            src_db.commit()
            for t in self.get_tables_in_state(TABLE_MISSING):
                if t.name not in pmap:
                    self.log.warning("Table %s not available on provider" % t.name)
                    continue
                pt = pmap[t.name]
                if pt.state != TABLE_OK: # or pt.custom_snapshot: # FIXME: does snapsnot matter?
                    self.log.info("Table %s not OK on provider, waiting" % t.name)
                    continue

                # dont allow more copies than configured
                if npossible == 0:
                    break
                npossible -= 1

                # drop all foreign keys to and from this table
                self.drop_fkeys(dst_db, t.name)

                # change state after fkeys are dropped thus allowing
                # failure inbetween
                self.change_table_state(dst_db, t, TABLE_IN_COPY)

                # the copy _may_ happen immidiately
                self.launch_copy(t)

                # there cannot be interesting events in current batch
                # but maybe there's several tables, lets do them in one go
                ret = SYNC_LOOP
        
        return ret


    def sync_from_copy_thread(self, cnt, src_db, dst_db):
        "Copy thread sync logic."

        # This operates on single table
        t = self.table_map[self.copy_table_name]

        if t.state == TABLE_DO_SYNC:
            # main thread is waiting, catch up, then handle over
            if self.cur_tick == t.sync_tick_id:
                self.change_table_state(dst_db, t, TABLE_OK)
                return SYNC_EXIT
            elif self.cur_tick < t.sync_tick_id:
                return SYNC_OK
            else:
                self.log.error("copy_sync: cur_tick=%d sync_tick=%d" % (
                                self.cur_tick, t.sync_tick_id))
                raise Exception('Invalid table state')
        elif t.state == TABLE_WANNA_SYNC:
            # wait for main thread to react
            return SYNC_LOOP
        elif t.state == TABLE_CATCHING_UP:

            # partition merging
            if t.copy_role == 'wait-replay':
                return SYNC_LOOP

            # is there more work?
            if self.work_state:
                return SYNC_OK

            # seems we have catched up
            self.change_table_state(dst_db, t, TABLE_WANNA_SYNC, self.cur_tick)
            return SYNC_LOOP
        elif t.state == TABLE_IN_COPY:
            # table is not copied yet, do it
            self.do_copy(t, src_db, dst_db)

            # forget previous value
            self.work_state = 1

            return SYNC_LOOP
        else:
            # nothing to do
            return SYNC_EXIT

    def do_copy(self, tbl, src_db, dst_db):
        """Callback for actual copy implementation."""
        raise Exception('do_copy not implemented')

    def process_remote_event(self, src_curs, dst_curs, ev):
        """handle one event"""
        self.log.debug("New event: id=%s / type=%s / data=%s / extra1=%s" % (ev.id, ev.type, ev.data, ev.extra1))
        if ev.type in ('I', 'U', 'D'):
            self.handle_data_event(ev, dst_curs)
            ev.tag_done()
        elif ev.type[:2] in ('I:', 'U:', 'D:'):
            self.handle_urlenc_event(ev, dst_curs)
            ev.tag_done()
        elif ev.type == "TRUNCATE":
            self.flush_sql(dst_curs)
            self.handle_truncate_event(ev, dst_curs)
            ev.tag_done()
        elif ev.type == 'EXECUTE':
            self.flush_sql(dst_curs)
            self.handle_execute_event(ev, dst_curs)
            ev.tag_done()
        elif ev.type == 'londiste.add-table':
            self.flush_sql(dst_curs)
            self.add_set_table(dst_curs, ev.data)
            ev.tag_done()
        elif ev.type == 'londiste.remove-table':
            self.flush_sql(dst_curs)
            self.remove_set_table(dst_curs, ev.data)
            ev.tag_done()
        elif ev.type == 'londiste.remove-seq':
            self.flush_sql(dst_curs)
            self.remove_set_seq(dst_curs, ev.data)
            ev.tag_done()
        elif ev.type == 'londiste.update-seq':
            self.flush_sql(dst_curs)
            self.update_seq(dst_curs, ev)
            ev.tag_done()
        else:
            CascadedWorker.process_remote_event(self, src_curs, dst_curs, ev)

    def handle_data_event(self, ev, dst_curs):
        """handle one data event"""
        t = self.get_table_by_name(ev.extra1)
        if t and t.interesting(ev, self.cur_tick, self.copy_thread):
            # buffer SQL statements, then send them together
            fqname = skytools.quote_fqident(ev.extra1)
            fmt = self.sql_command[ev.type]
            sql = fmt % (fqname, ev.data)

            self.apply_sql(sql, dst_curs)
        else:
            self.stat_increase('ignored_events')

    def handle_urlenc_event(self, ev, dst_curs):
        """handle one truncate event"""
        t = self.get_table_by_name(ev.extra1)
        if not t or not t.interesting(ev, self.cur_tick, self.copy_thread):
            self.stat_increase('ignored_events')
            return
        
        # parse event
        pklist = ev.type[2:].split(',')
        row = skytools.db_urldecode(ev.data)
        op = ev.type[0]
        tbl = ev.extra1

        # generate sql
        if op == 'I':
            sql = skytools.mk_insert_sql(row, tbl, pklist)
        elif op == 'U':
            sql = skytools.mk_update_sql(row, tbl, pklist)
        elif op == 'D':
            sql = skytools.mk_delete_sql(row, tbl, pklist)
        else:
            raise Exception('bug: bad op')

        self.apply_sql(sql, dst_curs)

    def handle_truncate_event(self, ev, dst_curs):
        """handle one truncate event"""
        t = self.get_table_by_name(ev.extra1)
        if not t or not t.interesting(ev, self.cur_tick, self.copy_thread):
            self.stat_increase('ignored_events')
            return

        fqname = skytools.quote_fqident(ev.extra1)
        sql = "TRUNCATE %s;" % fqname
        self.apply_sql(sql, dst_curs)

    def handle_execute_event(self, ev, dst_curs):
        """handle one EXECUTE event"""

        if self.copy_thread:
            return

        # parse event
        fname = ev.extra1
        sql = ev.data

        # fixme: curs?
        q = "select * from londiste.execute_start(%s, %s, %s, false)"
        res = self.exec_cmd(dst_curs, q, [self.queue_name, fname, sql], commit = False)
        ret = res[0]['ret_code']
        if ret != 200:
            return
        for stmt in skytools.parse_statements(sql):
            dst_curs.execute(stmt)
        q = "select * from londiste.execute_finish(%s, %s)"
        self.exec_cmd(dst_curs, q, [self.queue_name, fname], commit = False)

    def apply_sql(self, sql, dst_curs):
        self.sql_list.append(sql)
        if len(self.sql_list) > 200:
            self.flush_sql(dst_curs)

    def flush_sql(self, dst_curs):
        """Send all buffered statements to DB."""

        if len(self.sql_list) == 0:
            return

        buf = "\n".join(self.sql_list)
        self.sql_list = []

        dst_curs.execute(buf)

    def interesting(self, ev):
        """See if event is interesting."""
        if ev.type not in ('I', 'U', 'D'):
            raise Exception('bug - bad event type in .interesting')
        t = self.get_table_by_name(ev.extra1)
        if t:
            return t.interesting(ev, self.cur_tick, self.copy_thread)
        else:
            return 0

    def add_set_table(self, dst_curs, tbl):
        """There was new table added to root, remember it."""

        q = "select londiste.global_add_table(%s, %s)"
        dst_curs.execute(q, [self.set_name, tbl])

    def remove_set_table(self, dst_curs, tbl):
        """There was table dropped from root, remember it."""
        if tbl in self.table_map:
            t = self.table_map[tbl]
            del self.table_map[tbl]
            self.table_list.remove(t)
        q = "select londiste.global_remove_table(%s, %s)"
        dst_curs.execute(q, [self.set_name, tbl])

    def remove_set_seq(self, dst_curs, seq):
        """There was seq dropped from root, remember it."""

        q = "select londiste.global_remove_seq(%s, %s)"
        dst_curs.execute(q, [self.set_name, seq])

    def load_table_state(self, curs):
        """Load table state from database.
        
        Todo: if all tables are OK, there is no need
        to load state on every batch.
        """

        q = "select * from londiste.get_table_list(%s)"
        curs.execute(q, [self.set_name])

        new_list = []
        new_map = {}
        for row in curs.dictfetchall():
            if not row['local']:
                continue
            t = self.get_table_by_name(row['table_name'])
            if not t:
                t = TableState(row['table_name'], self.log)
            t.loaded_state(row)
            new_list.append(t)
            new_map[t.name] = t

        self.table_list = new_list
        self.table_map = new_map

    def get_state_map(self, curs):
        """Get dict of table states."""

        q = "select * from londiste.get_table_list(%s)"
        curs.execute(q, [self.set_name])

        new_map = {}
        for row in curs.fetchall():
            if not row['local']:
                continue
            t = TableState(row['table_name'], self.log)
            t.loaded_state(row)
            new_map[t.name] = t
        return new_map

    def save_table_state(self, curs):
        """Store changed table state in database."""

        for t in self.table_list:
            if not t.changed:
                continue
            merge_state = t.render_state()
            self.log.info("storing state of %s: copy:%d new_state:%s" % (
                            t.name, self.copy_thread, merge_state))
            q = "select londiste.local_set_table_state(%s, %s, %s, %s)"
            curs.execute(q, [self.set_name,
                             t.name, t.str_snapshot, merge_state])
            t.changed = 0

    def change_table_state(self, dst_db, tbl, state, tick_id = None):
        """Chage state for table."""

        tbl.change_state(state, tick_id)
        self.save_table_state(dst_db.cursor())
        dst_db.commit()

        self.log.info("Table %s status changed to '%s'" % (
                      tbl.name, tbl.render_state()))

    def get_tables_in_state(self, state):
        "get all tables with specific state"

        for t in self.table_list:
            if t.state == state:
                yield t

    def get_table_by_name(self, name):
        """Returns cached state object."""
        if name.find('.') < 0:
            name = "public.%s" % name
        if name in self.table_map:
            return self.table_map[name]
        return None

    def launch_copy(self, tbl_stat):
        """Run paraller worker for copy."""
        self.log.info("Launching copy process")
        script = sys.argv[0]
        conf = self.cf.filename
        cmd = [script, conf, 'copy', tbl_stat.name, '-d', '-q']
        if self.options.verbose:
            cmd.append('-v')

        # let existing copy finish and clean its pidfile,
        # otherwise new copy will exit immidiately.
        # FIXME: should not happen on per-table pidfile ???
        copy_pidfile = "%s.copy.%s" % (self.pidfile, tbl_stat.name)
        while skytools.signal_pidfile(copy_pidfile, 0):
            self.log.warning("Waiting for existing copy to exit")
            time.sleep(2)

        self.log.debug("Launch args: "+repr(cmd))
        pid = os.spawnvp(os.P_NOWAIT, script, cmd)
        self.log.debug("Launch result: "+repr(pid))

    def sync_database_encodings(self, src_db, dst_db):
        """Make sure client_encoding is same on both side."""

        try:
            # psycopg2
            if src_db.encoding != dst_db.encoding:
                dst_db.set_client_encoding(src_db.encoding)
        except AttributeError:
            # psycopg1
            src_curs = src_db.cursor()
            dst_curs = dst_db.cursor()
            src_curs.execute("show client_encoding")
            src_enc = src_curs.fetchone()[0]
            dst_curs.execute("show client_encoding")
            dst_enc = dst_curs.fetchone()[0]
            if src_enc != dst_enc:
                dst_curs.execute("set client_encoding = %s", [src_enc])

    def copy_snapshot_cleanup(self, dst_db):
        """Remove unnecassary snapshot info from tables."""
        no_lag = not self.work_state
        changes = False
        for t in self.table_list:
            t.gc_snapshot(self.copy_thread, self.prev_tick, self.cur_tick, no_lag)
            if t.changed:
                changes = True

        if changes:
            self.save_table_state(dst_db.cursor())
            dst_db.commit()

    def restore_fkeys(self, dst_db):
        """Restore fkeys that have both tables on sync."""
        dst_curs = dst_db.cursor()
        # restore fkeys -- one at a time
        q = "select * from londiste.get_valid_pending_fkeys(%s)"
        dst_curs.execute(q, [self.set_name])
        fkey_list = dst_curs.dictfetchall()
        for row in fkey_list:
            self.log.info('Creating fkey: %(fkey_name)s (%(from_table)s --> %(to_table)s)' % row)
            q2 = "select londiste.restore_table_fkey(%(from_table)s, %(fkey_name)s)"
            dst_curs.execute(q2, row)
            dst_db.commit()
    
    def drop_fkeys(self, dst_db, table_name):
        """Drop all foreign keys to and from this table.

        They need to be dropped one at a time to avoid deadlocks with user code.
        """

        dst_curs = dst_db.cursor()
        q = "select * from londiste.find_table_fkeys(%s)"
        dst_curs.execute(q, [table_name])
        fkey_list = dst_curs.dictfetchall()
        for row in fkey_list:
            self.log.info('Dropping fkey: %s' % row['fkey_name'])
            q2 = "select londiste.drop_table_fkey(%(from_table)s, %(fkey_name)s)"
            dst_curs.execute(q2, row)
            dst_db.commit()
        
    def process_root_node(self, dst_db):
        """On root node send seq changes to queue."""

        CascadedWorker.process_root_node(self, dst_db)

        q = "select * from londiste.root_check_seqs(%s)"
        self.exec_cmd(dst_db, q, [self.queue_name])

    def update_seq(self, dst_curs, ev):
        if self.copy_thread:
            return

        val = int(ev.data)
        seq = ev.extra1
        q = "select * from londiste.global_update_seq(%s, %s, %s)"
        self.exec_cmd(dst_curs, q, [self.queue_name, seq, val])

    def copy_event(self, dst_curs, ev):
        # send only data events down (skipping seqs also)
        if ev.type[:9] in ('londiste.', 'EXECUTE', 'TRUNCATE'):
            return
        CascadedWorker.copy_event(self, dst_curs, ev)

if __name__ == '__main__':
    script = Replicator(sys.argv[1:])
    script.start()

