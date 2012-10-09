"""Cascaded worker.

CascadedConsumer that also maintains node.

"""

import sys, time, skytools

from pgq.cascade.consumer import CascadedConsumer
from pgq.producer import bulk_insert_events
from pgq.event import Event

__all__ = ['CascadedWorker']

class WorkerState:
    """Depending on node state decides on actions worker needs to do."""
    # node_type,
    # node_name, provider_node,
    # global_watermark, local_watermark
    # combined_queue, combined_type
    process_batch = 0       # handled in CascadedConsumer
    copy_events = 0         # ok
    global_wm_event = 0     # ok
    local_wm_publish = 1    # ok

    process_events = 0      # ok
    send_tick_event = 0     # ok
    wait_behind = 0         # ok
    process_tick_event = 0  # ok
    target_queue = ''       # ok
    keep_event_ids = 0      # ok
    create_tick = 0         # ok
    filtered_copy = 0       # ok
    process_global_wm = 0   # ok

    sync_watermark = 0      # ?
    wm_sync_nodes = []

    def __init__(self, queue_name, nst):
        self.node_type = nst['node_type']
        self.node_name = nst['node_name']
        self.local_watermark = nst['local_watermark']
        self.global_watermark = nst['global_watermark']

        self.node_attrs = {}
        attrs = nst.get('node_attrs', '')
        if attrs:
            self.node_attrs = skytools.db_urldecode(attrs)

        ntype = nst['node_type']
        ctype = nst['combined_type']
        if ntype == 'root':
            self.global_wm_event = 1
            self.local_wm_publish = 0
        elif ntype == 'branch':
            self.target_queue = queue_name
            self.process_batch = 1
            self.process_events = 1
            self.copy_events = 1
            self.process_tick_event = 1
            self.keep_event_ids = 1
            self.create_tick = 1
            if 'sync_watermark' in self.node_attrs:
                slist = self.node_attrs['sync_watermark']
                self.sync_watermark = 1
                self.wm_sync_nodes = slist.split(',')
            else:
                self.process_global_wm = 1
        elif ntype == 'leaf' and not ctype:
            self.process_batch = 1
            self.process_events = 1
        elif ntype == 'leaf' and ctype:
            self.target_queue = nst['combined_queue']
            if ctype == 'root':
                self.process_batch = 1
                self.process_events = 1
                self.copy_events = 1
                self.filtered_copy = 1
                self.send_tick_event = 1
            elif ctype == 'branch':
                self.process_batch = 1
                self.wait_behind = 1
            else:
                raise Exception('invalid state 1')
        else:
            raise Exception('invalid state 2')
        if ctype and ntype != 'leaf':
            raise Exception('invalid state 3')

class CascadedWorker(CascadedConsumer):
    """CascadedWorker base class.

    Config fragment::

        ## Parameters for pgq.CascadedWorker ##

        # how often the root node should push wm downstream (seconds)
        #global_wm_publish_period = 300

        # how often the nodes should report their wm upstream (seconds)
        #local_wm_publish_period = 300
    """

    global_wm_publish_time = 0
    global_wm_publish_period = 5 * 60

    local_wm_publish_time = 0
    local_wm_publish_period = 5 * 60

    max_evbuf = 500
    cur_event_seq = 0
    cur_max_id = 0
    seq_buffer = 10000

    main_worker = True

    _worker_state = None
    ev_buf = []

    real_global_wm = None

    def __init__(self, service_name, db_name, args):
        """Initialize new consumer.
        
        @param service_name: service_name for DBScript
        @param db_name: target database name for get_database()
        @param args: cmdline args for DBScript
        """

        CascadedConsumer.__init__(self, service_name, db_name, args)

    def reload(self):
        CascadedConsumer.reload(self)

        self.global_wm_publish_period = self.cf.getfloat('global_wm_publish_period', CascadedWorker.global_wm_publish_period)
        self.local_wm_publish_period = self.cf.getfloat('local_wm_publish_period', CascadedWorker.local_wm_publish_period)

    def process_remote_batch(self, src_db, tick_id, event_list, dst_db):
        """Worker-specific event processing."""
        self.ev_buf = []
        max_id = 0
        st = self._worker_state

        if st.wait_behind:
            self.wait_for_tick(dst_db, tick_id)

        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()
        for ev in event_list:
            if st.copy_events:
                self.copy_event(dst_curs, ev, st.filtered_copy)
            if ev.ev_type.split('.', 1)[0] in ("pgq", "londiste"):
                # process cascade events even on waiting leaf node
                self.process_remote_event(src_curs, dst_curs, ev)
            else:
                if st.process_events:
                    self.process_remote_event(src_curs, dst_curs, ev)
            if ev.ev_id > max_id:
                max_id = ev.ev_id
        if max_id > self.cur_max_id:
            self.cur_max_id = max_id

    def wait_for_tick(self, dst_db, tick_id):
        """On combined-branch leaf needs to wait from tick
        to appear from combined-root.
        """
        while 1:
            cst = self._consumer_state
            if cst['completed_tick'] >= tick_id:
                return
            self.sleep(10 * self.loop_delay)
            self._consumer_state = self.refresh_state(dst_db)
            if not self.looping:
                sys.exit(0)

    def is_batch_done(self, state, batch_info, dst_db):
        wst = self._worker_state

        # on combined-branch the target can get several batches ahead
        if wst.wait_behind:
            # let the wait-behind logic track ticks
            return False

        # check if events have processed
        done = CascadedConsumer.is_batch_done(self, state, batch_info, dst_db)
        if not wst.create_tick:
            return done
        if not done:
            return False

        # check if tick is done - it happens in separate tx

        # fetch last tick from target queue
        q = "select t.tick_id from pgq.tick t, pgq.queue q"\
            " where t.tick_queue = q.queue_id and q.queue_name = %s"\
            " order by t.tick_queue desc, t.tick_id desc"\
            " limit 1"
        curs = dst_db.cursor()
        curs.execute(q, [self.queue_name])
        last_tick = curs.fetchone()['tick_id']
        dst_db.commit()

        # insert tick if missing
        cur_tick = batch_info['tick_id']
        if last_tick != cur_tick:
            prev_tick = batch_info['prev_tick_id']
            tick_time = batch_info['batch_end']
            if last_tick != prev_tick:
                raise Exception('is_batch_done: last branch tick = %d, expected %d or %d' % (
                                last_tick, prev_tick, cur_tick))
            self.create_branch_tick(dst_db, cur_tick, tick_time)
        return True

    def publish_local_wm(self, src_db, dst_db):
        """Send local watermark to provider.
        """

        t = time.time()
        if t - self.local_wm_publish_time < self.local_wm_publish_period:
            return

        st = self._worker_state
        wm = st.local_watermark
        if st.sync_watermark:
            # dont send local watermark upstream
            wm = self.batch_info['prev_tick_id']
        elif wm > self.batch_info['cur_tick_id']:
            # in wait-behind-leaf case, the wm from target can be
            # ahead from source queue, use current batch then
            wm = self.batch_info['cur_tick_id']

        self.log.debug("Publishing local watermark: %d" % wm)
        src_curs = src_db.cursor()
        q = "select * from pgq_node.set_subscriber_watermark(%s, %s, %s)"
        src_curs.execute(q, [self.pgq_queue_name, st.node_name, wm])
        src_db.commit()

        # if next part fails, dont repeat it immediately
        self.local_wm_publish_time = t

        if st.sync_watermark and self.real_global_wm is not None:
            # instead sync 'global-watermark' with specific nodes
            dst_curs = dst_db.cursor()
            nmap = self._get_node_map(dst_curs)
            dst_db.commit()

            # local lowest
            wm = st.local_watermark

            # the global-watermark in subtree can stay behind
            # upstream global-watermark, but must not go ahead
            if self.real_global_wm < wm:
                wm = self.real_global_wm

            for node in st.wm_sync_nodes:
                if node == st.node_name:
                    continue
                if node not in nmap:
                    # dont ignore missing nodes - cluster may be partially set up
                    self.log.warning('Unknown node in sync_watermark list: %s' % node)
                    return
                n = nmap[node]
                if n['dead']:
                    # ignore dead nodes
                    continue
                wmdb = self.get_database('wmdb', connstr = n['node_location'], autocommit = 1)
                wmcurs = wmdb.cursor()
                q = 'select local_watermark from pgq_node.get_node_info(%s)'
                wmcurs.execute(q, [self.queue_name])
                row = wmcurs.fetchone()
                if not row:
                    # partially set up node?
                    self.log.warning('Node not working: %s' % node)
                elif row['local_watermark'] < wm:
                    # keep lowest wm
                    wm = row['local_watermark']
                self.close_database('wmdb')

            # now we have lowest wm, store it
            q = "select pgq_node.set_global_watermark(%s, %s)"
            dst_curs.execute(q, [self.queue_name, wm])
            dst_db.commit()

    def _get_node_map(self, curs):
        q = "select node_name, node_location, dead from pgq_node.get_queue_locations(%s)"
        curs.execute(q, [self.queue_name])
        res = {}
        for row in curs.fetchall():
            res[row['node_name']] = row
        return res

    def process_remote_event(self, src_curs, dst_curs, ev):
        """Handle cascading events.
        """

        if ev.retry:
            raise Exception('CascadedWorker must not get retry events')

        # non cascade events send to CascadedConsumer to error out
        if ev.ev_type[:4] != 'pgq.':
            CascadedConsumer.process_remote_event(self, src_curs, dst_curs, ev)
            return

        # ignore cascade events if not main worker
        if not self.main_worker:
            return

        # check if for right queue
        t = ev.ev_type
        if ev.ev_extra1 != self.pgq_queue_name and t != "pgq.tick-id":
            raise Exception("bad event in queue: "+str(ev))

        self.log.debug("got cascade event: %s(%s)" % (t, ev.ev_data))
        st = self._worker_state
        if t == "pgq.location-info":
            node = ev.ev_data
            loc = ev.ev_extra2
            dead = ev.ev_extra3
            q = "select * from pgq_node.register_location(%s, %s, %s, %s)"
            dst_curs.execute(q, [self.pgq_queue_name, node, loc, dead])
        elif t == "pgq.unregister-location":
            node = ev.ev_data
            q = "select * from pgq_node.unregister_location(%s, %s)"
            dst_curs.execute(q, [self.pgq_queue_name, node])
        elif t == "pgq.global-watermark":
            if st.sync_watermark:
                tick_id = int(ev.ev_data)
                self.log.debug('Half-ignoring global watermark %d', tick_id)
                self.real_global_wm = tick_id
            elif st.process_global_wm:
                tick_id = int(ev.ev_data)
                q = "select * from pgq_node.set_global_watermark(%s, %s)"
                dst_curs.execute(q, [self.pgq_queue_name, tick_id])
        elif t == "pgq.tick-id":
            tick_id = int(ev.ev_data)
            if ev.ev_extra1 == self.pgq_queue_name:
                raise Exception('tick-id event for own queue?')
            if st.process_tick_event:
                q = "select * from pgq_node.set_partition_watermark(%s, %s, %s)"
                dst_curs.execute(q, [self.pgq_queue_name, ev.ev_extra1, tick_id])
        else:
            raise Exception("unknown cascade event: %s" % t)

    def finish_remote_batch(self, src_db, dst_db, tick_id):
        """Worker-specific cleanup on target node.
        """

        # merge-leaf on branch should not update tick pos
        st = self._worker_state
        if st.wait_behind:
            dst_db.commit()

            # still need to publish wm info
            if st.local_wm_publish and self.main_worker:
                self.publish_local_wm(src_db, dst_db)

            return

        if self.main_worker:
            dst_curs = dst_db.cursor()

            self.flush_events(dst_curs)

            # send tick event into queue
            if st.send_tick_event:
                q = "select pgq.insert_event(%s, 'pgq.tick-id', %s, %s, null, null, null)"
                dst_curs.execute(q, [st.target_queue, str(tick_id), self.pgq_queue_name])

        CascadedConsumer.finish_remote_batch(self, src_db, dst_db, tick_id)

        if self.main_worker:
            if st.create_tick:
                # create actual tick
                tick_id = self.batch_info['tick_id']
                tick_time = self.batch_info['batch_end']
                self.create_branch_tick(dst_db, tick_id, tick_time)
            if st.local_wm_publish:
                self.publish_local_wm(src_db, dst_db)

    def create_branch_tick(self, dst_db, tick_id, tick_time):
        q = "select pgq.ticker(%s, %s, %s, %s)"
        # execute it in autocommit mode
        ilev = dst_db.isolation_level
        dst_db.set_isolation_level(0)
        dst_curs = dst_db.cursor()
        dst_curs.execute(q, [self.pgq_queue_name, tick_id, tick_time, self.cur_max_id])
        dst_db.set_isolation_level(ilev)

    def copy_event(self, dst_curs, ev, filtered_copy):
        """Add event to copy buffer.
        """
        if not self.main_worker:
            return
        if filtered_copy:
            if ev.type[:4] == "pgq.":
                return
        if len(self.ev_buf) >= self.max_evbuf:
            self.flush_events(dst_curs)

        if ev.type == 'pgq.global-watermark':
            st = self._worker_state
            if st.sync_watermark:
                # replace payload with synced global watermark
                row = ev._event_row.copy()
                row['ev_data'] = str(st.global_watermark)
                ev = Event(self.queue_name, row)
        self.ev_buf.append(ev)

    def flush_events(self, dst_curs):
        """Send copy buffer to target queue.
        """
        if len(self.ev_buf) == 0:
            return
        flds = ['ev_time', 'ev_type', 'ev_data', 'ev_extra1',
                'ev_extra2', 'ev_extra3', 'ev_extra4']
        st = self._worker_state
        if st.keep_event_ids:
            flds.append('ev_id')
        bulk_insert_events(dst_curs, self.ev_buf, flds, st.target_queue)
        self.ev_buf = []

    def refresh_state(self, dst_db, full_logic = True):
        """Load also node state from target node.
        """
        res = CascadedConsumer.refresh_state(self, dst_db, full_logic)
        q = "select * from pgq_node.get_node_info(%s)"
        st = self.exec_cmd(dst_db, q, [ self.pgq_queue_name ])
        self._worker_state = WorkerState(self.pgq_queue_name, st[0])
        return res

    def process_root_node(self, dst_db):
        """On root node send global watermark downstream.
        """

        CascadedConsumer.process_root_node(self, dst_db)

        t = time.time()
        if t - self.global_wm_publish_time < self.global_wm_publish_period:
            return

        self.log.debug("Publishing global watermark")
        dst_curs = dst_db.cursor()
        q = "select * from pgq_node.set_global_watermark(%s, NULL)"
        dst_curs.execute(q, [self.pgq_queue_name])
        dst_db.commit()
        self.global_wm_publish_time = t

