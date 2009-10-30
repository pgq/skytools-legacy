#! /usr/bin/env python

"""Basic replication core."""

import sys, os, time
import skytools, pgq

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
        # only one table is allowed to have in-progress copy
        if self.in_progress_copy() > 1:
            raise Exception('Bad table state')

    def in_progress_copy(self):
        """ return how many tables currently having in-progress copy """
        return self.copy + self.catching_up + self.wanna_sync + self.do_sync

    def get_running_copy_state(self):
        """ return TABLE_STATE of current running COPY table """

        if self.in_progress_copy() == 0:
            return None

        if self.copy:
            return TABLE_IN_COPY
        elif self.catching_up:
            return TABLE_CATCHING_UP
        elif self.wanna_sync:
            return TABLE_WANNA_SYNC
        elif self.do_sync:
            return TABLE_DO_SYNC

class TableState(object):
    """Keeps state about one table."""
    def __init__(self, name, log):
        self.name = name
        self.log = log
        self.forget()
        self.changed = 0
        self.skip_truncate = False

    def forget(self):
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

    def loaded_state(self, merge_state, str_snapshot, skip_truncate):
        self.log.debug("loaded_state: %s: %s / %s" % (
                       self.name, merge_state, str_snapshot))
        self.change_snapshot(str_snapshot, 0)
        self.state = self.parse_state(merge_state)
        self.changed = 0
        self.skip_truncate = skip_truncate
        if merge_state == "?":
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

class SeqCache(object):
    def __init__(self):
        self.seq_list = []
        self.fq_seq_list = []
        self.val_cache = {}

    def set_seq_list(self, seq_list):
        self.seq_list = seq_list
        self.fq_seq_list = [skytools.quote_fqident(s) for s in seq_list]
        new_cache = {}
        for seq in seq_list:
            val = self.val_cache.get(seq)
            if val:
                new_cache[seq] = val
        self.val_cache = new_cache

    def resync(self, src_curs, dst_curs):
        if len(self.seq_list) == 0:
            return
        dat = ".last_value, ".join(self.fq_seq_list)
        dat += ".last_value"
        q = "select %s from %s" % (dat, ",".join(self.fq_seq_list))
        src_curs.execute(q)
        row = src_curs.fetchone()
        for i in range(len(self.seq_list)):
            seq = self.seq_list[i]
            fqseq = self.fq_seq_list[i]
            cur = row[i]
            old = self.val_cache.get(seq)
            if old != cur:
                q = "select setval(%s, %s)"
                dst_curs.execute(q, [fqseq, cur])
                self.val_cache[seq] = cur

class Replicator(pgq.SerialConsumer):
    """Replication core."""

    sql_command = {
        'I': "insert into %s %s;",
        'U': "update only %s set %s;",
        'D': "delete from only %s where %s;",
    }

    # batch info
    cur_tick = 0
    prev_tick = 0

    def __init__(self, args):
        pgq.SerialConsumer.__init__(self, 'londiste', 'provider_db', 'subscriber_db', args)

        # where get/set_last_tick() function reside for SerialConsumer().
        # default is pgq_ext, but lets keep londiste code under one schema
        self.dst_schema = "londiste"

        self.table_list = []
        self.table_map = {}

        self.copy_thread = 0
        self.maint_time = 0
        self.checked_copy = False
        self.seq_cache = SeqCache()
        self.maint_delay = self.cf.getint('maint_delay', 600)
        self.mirror_queue = self.cf.get('mirror_queue', '')

    def process_remote_batch(self, src_db, batch_id, ev_list, dst_db):
        "All work for a batch.  Entry point from SerialConsumer."

        # this part can play freely with transactions

        dst_curs = dst_db.cursor()
        
        self.cur_tick = self.cur_batch_info['tick_id']
        self.prev_tick = self.cur_batch_info['prev_tick_id']

        self.load_table_state(dst_curs)
        self.sync_tables(dst_db)

        self.copy_snapshot_cleanup(dst_db)

        # only main thread is allowed to restore fkeys
        if not self.copy_thread:
            self.restore_fkeys(dst_db)

        # now the actual event processing happens.
        # they must be done all in one tx in dst side
        # and the transaction must be kept open so that
        # the SerialConsumer can save last tick and commit.

        self.sync_database_encodings(src_db, dst_db)

        self.handle_seqs(dst_curs)
        self.handle_events(dst_curs, ev_list)
        self.save_table_state(dst_curs)

    def handle_seqs(self, dst_curs):
        if self.copy_thread:
            return

        q = "select * from londiste.subscriber_get_seq_list(%s)"
        dst_curs.execute(q, [self.pgq_queue_name])
        seq_list = []
        for row in dst_curs.fetchall():
            seq_list.append(row[0])

        self.seq_cache.set_seq_list(seq_list)

        src_curs = self.get_database('provider_db').cursor()
        self.seq_cache.resync(src_curs, dst_curs)

    def sync_tables(self, dst_db):
        """Table sync loop.
        
        Calls appropriate handles, which is expected to
        return one of SYNC_* constants."""

        self.log.debug('Sync tables')
        while 1:
            cnt = Counter(self.table_list)
            if self.copy_thread:
                res = self.sync_from_copy_thread(cnt, dst_db)
            else:
                res = self.sync_from_main_thread(cnt, dst_db)

            if res == SYNC_EXIT:
                self.log.debug('Sync tables: exit')
                self.detach()
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
    
    def sync_from_main_thread(self, cnt, dst_db):
        "Main thread sync logic."

        if not self.checked_copy:
            self.relaunch_copy(cnt)
            self.checked_copy = True
        
        #
        # decide what to do - order is important
        #
        if cnt.do_sync:
            # wait for copy thread to catch up
            return SYNC_LOOP
        elif cnt.wanna_sync:
            # copy thread wants sync, if not behind, do it
            t = self.get_table_by_state(TABLE_WANNA_SYNC)
            if self.cur_tick >= t.sync_tick_id:
                self.change_table_state(dst_db, t, TABLE_DO_SYNC, self.cur_tick)
                return SYNC_LOOP
            else:
                return SYNC_OK
        elif cnt.catching_up:
            # active copy, dont worry
            return SYNC_OK
        elif cnt.copy:
            # active copy, dont worry
            return SYNC_OK
        elif cnt.missing:
            # seems there is no active copy thread, launch new
            t = self.get_table_by_state(TABLE_MISSING)

            # drop all foreign keys to and from this table
            self.drop_fkeys(dst_db, t.name)

            # change state after fkeys are dropped thus allowing
            # failure inbetween
            self.change_table_state(dst_db, t, TABLE_IN_COPY)

            # the copy _may_ happen immidiately
            self.launch_copy(t)

            # there cannot be interesting events in current batch
            # but maybe there's several tables, lets do them in one go
            return SYNC_LOOP
        else:
            # seems everything is in sync
            return SYNC_OK

    def sync_from_copy_thread(self, cnt, dst_db):
        "Copy thread sync logic."

        #
        # decide what to do - order is important
        #
        if cnt.do_sync:
            # main thread is waiting, catch up, then handle over
            t = self.get_table_by_state(TABLE_DO_SYNC)
            if self.cur_tick == t.sync_tick_id:
                self.change_table_state(dst_db, t, TABLE_OK)
                return SYNC_EXIT
            elif self.cur_tick < t.sync_tick_id:
                return SYNC_OK
            else:
                self.log.error("copy_sync: cur_tick=%d sync_tick=%d" % (
                                self.cur_tick, t.sync_tick_id))
                raise Exception('Invalid table state')
        elif cnt.wanna_sync:
            # wait for main thread to react
            return SYNC_LOOP
        elif cnt.catching_up:
            # is there more work?
            if self.work_state:
                return SYNC_OK

            # seems we have catched up
            t = self.get_table_by_state(TABLE_CATCHING_UP)
            self.change_table_state(dst_db, t, TABLE_WANNA_SYNC, self.cur_tick)
            return SYNC_LOOP
        elif cnt.copy:
            # table is not copied yet, do it
            t = self.get_table_by_state(TABLE_IN_COPY)
            self.do_copy(t)

            # forget previous value
            self.work_state = 1

            return SYNC_LOOP
        else:
            # nothing to do
            return SYNC_EXIT

    def handle_events(self, dst_curs, ev_list):
        "Actual event processing happens here."

        ignored_events = 0
        self.sql_list = []
        mirror_list = []
        for ev in ev_list:
            if not self.interesting(ev):
                ignored_events += 1
                ev.tag_done()
                continue
            
            if ev.type in ('I', 'U', 'D'):
                self.handle_data_event(ev, dst_curs)
            else:
                self.handle_system_event(ev, dst_curs)

            if self.mirror_queue:
                mirror_list.append(ev)

        # finalize table changes
        self.flush_sql(dst_curs)
        self.stat_add('ignored', ignored_events)

        # put events into mirror queue if requested
        if self.mirror_queue:
            self.fill_mirror_queue(mirror_list, dst_curs)

    def handle_data_event(self, ev, dst_curs):
        # buffer SQL statements, then send them together
        fqname = skytools.quote_fqident(ev.extra1)
        fmt = self.sql_command[ev.type]
        sql = fmt % (fqname, ev.data)
        self.sql_list.append(sql)
        if len(self.sql_list) > 200:
            self.flush_sql(dst_curs)
        ev.tag_done()

    def flush_sql(self, dst_curs):
        # send all buffered statements at once

        if len(self.sql_list) == 0:
            return

        buf = "\n".join(self.sql_list)
        self.sql_list = []

        dst_curs.execute(buf)

    def interesting(self, ev):
        if ev.type not in ('I', 'U', 'D'):
            return 1
        t = self.get_table_by_name(ev.extra1)
        if t:
            return t.interesting(ev, self.cur_tick, self.copy_thread)
        else:
            return 0

    def handle_system_event(self, ev, dst_curs):
        "System event."

        if ev.type == "T":
            self.log.info("got new table event: "+ev.data)
            # check tables to be dropped
            name_list = []
            for name in ev.data.split(','):
                name_list.append(name.strip())

            del_list = []
            for tbl in self.table_list:
                if tbl.name in name_list:
                    continue
                del_list.append(tbl)

            # separate loop to avoid changing while iterating
            for tbl in del_list:
                self.log.info("Removing table %s from set" % tbl.name)
                self.remove_table(tbl, dst_curs)

            ev.tag_done()
        else:
            self.log.warning("Unknows op %s" % ev.type)
            ev.tag_failed("Unknown operation")

    def remove_table(self, tbl, dst_curs):
        del self.table_map[tbl.name]
        self.table_list.remove(tbl)
        q = "select londiste.subscriber_remove_table(%s, %s)"
        dst_curs.execute(q, [self.pgq_queue_name, tbl.name])

    def load_table_state(self, curs):
        """Load table state from database.
        
        Todo: if all tables are OK, there is no need
        to load state on every batch.
        """

        q = "select table_name, snapshot, merge_state, skip_truncate"\
            "  from londiste.subscriber_get_table_list(%s)"
        curs.execute(q, [self.pgq_queue_name])

        new_list = []
        new_map = {}
        for row in curs.dictfetchall():
            t = self.get_table_by_name(row['table_name'])
            if not t:
                t = TableState(row['table_name'], self.log)
            t.loaded_state(row['merge_state'], row['snapshot'], row['skip_truncate'])
            new_list.append(t)
            new_map[t.name] = t

        self.table_list = new_list
        self.table_map = new_map

    def save_table_state(self, curs):
        """Store changed table state in database."""

        got_changes = 0
        for t in self.table_list:
            if not t.changed:
                continue
            merge_state = t.render_state()
            self.log.info("storing state of %s: copy:%d new_state:%s" % (
                            t.name, self.copy_thread, merge_state))
            q = "select londiste.subscriber_set_table_state(%s, %s, %s, %s)"
            curs.execute(q, [self.pgq_queue_name,
                             t.name, t.str_snapshot, merge_state])
            t.changed = 0
            got_changes = 1

    def change_table_state(self, dst_db, tbl, state, tick_id = None):
        tbl.change_state(state, tick_id)
        self.save_table_state(dst_db.cursor())
        dst_db.commit()

        self.log.info("Table %s status changed to '%s'" % (
                      tbl.name, tbl.render_state()))

    def get_table_by_state(self, state):
        "get first table with specific state"

        for t in self.table_list:
            if t.state == state:
                return t
        raise Exception('No table was found with state: %d' % state)

    def get_table_by_name(self, name):
        if name.find('.') < 0:
            name = "public.%s" % name
        if name in self.table_map:
            return self.table_map[name]
        return None

    def fill_mirror_queue(self, ev_list, dst_curs):
        # insert events
        rows = []
        fields = ['ev_type', 'ev_data', 'ev_extra1']
        for ev in mirror_list:
            rows.append((ev.type, ev.data, ev.extra1))
        pgq.bulk_insert_events(dst_curs, rows, fields, self.mirror_queue)

        # create tick
        q = "select pgq.ticker(%s, %s)"
        dst_curs.execute(q, [self.mirror_queue, self.cur_tick])

    def launch_copy(self, tbl_stat):
        self.log.info("Launching copy process")
        script = sys.argv[0]
        conf = self.cf.filename
        if self.options.verbose:
            cmd = "%s -d -v %s copy"
        else:
            cmd = "%s -d %s copy"
        cmd = cmd % (script, conf)

        # wait until existing copy finishes
        copy_pidfile = self.pidfile + ".copy"
        while skytools.signal_pidfile(copy_pidfile, 0):
            self.log.info("Waiting for existing copy to exit")
            time.sleep(2)
            
        self.log.debug("Launch args: "+repr(cmd))
        res = os.system(cmd)
        self.log.debug("Launch result: "+repr(res))

    def relaunch_copy(self, cnt):
        """ check if a copy was killed before completion """

        # We decide to force to run a COPY if:
        #  - a table is in "copy" state
        #  - copy pidfile either does not exists or matches no running pid

        self.log.debug("Sync(main) in_progress_copy = %d" % (cnt.in_progress_copy()))

        if cnt.in_progress_copy() == 0:
            return

        copy_pidfile = self.pidfile + ".copy"
        if skytools.signal_pidfile(copy_pidfile, 0):
            # copy is running
            return

        self.log.info("Table have in-progress-copy but no process")
        
        if os.path.isfile(copy_pidfile):
            self.log.debug("removing stale copy pid file %s" \
                           % copy_pidfile)
            os.remove(copy_pidfile)

        state = cnt.get_running_copy_state()
        if state:
            t = self.get_table_by_state(state)
            self.log.debug("launch copy for %s in state %s" % (str(t), str(state)))
            self.launch_copy(t)
        else:
            self.log.error("Can't find copy-in-progress table " +\
                           "state to re-launch stale copy")
    

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
        q = "select * from londiste.subscriber_get_queue_valid_pending_fkeys(%s)"
        dst_curs.execute(q, [self.pgq_queue_name])
        list = dst_curs.dictfetchall()
        for row in list:
            self.log.info('Creating fkey: %(fkey_name)s (%(from_table)s --> %(to_table)s)' % row)
            q2 = "select londiste.subscriber_restore_table_fkey(%(from_table)s, %(fkey_name)s)"
            dst_curs.execute(q2, row)
            dst_db.commit()
    
    def drop_fkeys(self, dst_db, table_name):
        # drop all foreign keys to and from this table
        # they need to be dropped one at a time to avoid deadlocks with user code
        dst_curs = dst_db.cursor()
        q = "select * from londiste.find_table_fkeys(%s)"
        dst_curs.execute(q, [table_name])
        list = dst_curs.dictfetchall()
        for row in list:
            self.log.info('Dropping fkey: %s' % row['fkey_name'])
            q2 = "select londiste.subscriber_drop_table_fkey(%(from_table)s, %(fkey_name)s)"
            dst_curs.execute(q2, row)
            dst_db.commit()
        
if __name__ == '__main__':
    script = Replicator(sys.argv[1:])
    script.start()

