"""Cascaded worker.

CascadedConsumer that also maintains node.

"""

import time

from pgq.cascade.consumer import CascadedConsumer
from pgq.producer import bulk_insert_events

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
    def __init__(self, queue_name, nst):
        self.node_type = nst['node_type']
        self.node_name = nst['node_name']
        self.local_watermark = nst['local_watermark']
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
    ev_buf = None

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
            if ev.ev_type[:4] == "pgq.":
                # process cascade events even on waiting leaf node
                self.process_remote_event(src_curs, dst_curs, ev)
            else:
                if st.process_events:
                    self.process_remote_event(src_curs, dst_curs, ev)
            if ev.ev_id > max_id:
                max_id = ev.ev_id
        if st.local_wm_publish:
            self.publish_local_wm(src_db)
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
            time.sleep(10 * self.loop_delay)
            self._consumer_state = self.refresh_state(dst_db)

    def publish_local_wm(self, src_db):
        """Send local watermark to provider.
        """
        if not self.main_worker:
            return
        t = time.time()
        if t - self.local_wm_publish_time < self.local_wm_publish_period:
            return

        st = self._worker_state
        self.log.debug("Publishing local watermark: %d" % st.local_watermark)
        src_curs = src_db.cursor()
        q = "select * from pgq_node.set_subscriber_watermark(%s, %s, %s)"
        src_curs.execute(q, [self.pgq_queue_name, st.node_name, st.local_watermark])
        self.local_wm_publish_time = t

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

        self.log.info("got cascade event: %s" % t)
        if t == "pgq.location-info":
            node = ev.ev_data
            loc = ev.ev_extra2
            dead = ev.ev_extra3
            q = "select * from pgq_node.register_location(%s, %s, %s, %s)"
            dst_curs.execute(q, [self.pgq_queue_name, node, loc, dead])
        elif t == "pgq.global-watermark":
            tick_id = int(ev.ev_data)
            q = "select * from pgq_node.set_global_watermark(%s, %s)"
            dst_curs.execute(q, [self.pgq_queue_name, tick_id])
        elif t == "pgq.tick-id":
            tick_id = int(ev.ev_data)
            if ev.ev_extra1 == self.pgq_queue_name:
                raise Exception('tick-id event for own queue?')
            st = self._worker_state
            if st.process_tick_event:
                q = "select * from pgq_node.set_partition_watermark(%s, %s, %s)"
                dst_curs.execute(q, [self.pgq_queue_name, ev.ev_extra1, tick_id])
        else:
            raise Exception("unknown cascade event: %s" % t)

    def finish_remote_batch(self, src_db, dst_db, tick_id):
        """Worker-specific cleanup on target node.
        """

        if self.main_worker:
            st = self._worker_state
            dst_curs = dst_db.cursor()

            self.flush_events(dst_curs)

            # send tick event into queue
            if st.send_tick_event:
                q = "select pgq.insert_event(%s, 'pgq.tick-id', %s, %s, null, null, null)"
                dst_curs.execute(q, [st.target_queue, str(tick_id), self.pgq_queue_name])
            if st.create_tick:
                # create actual tick
                tick_id = self._batch_info['tick_id']
                tick_time = self._batch_info['batch_end']
                q = "select pgq.ticker(%s, %s, %s, %s)"
                dst_curs.execute(q, [self.pgq_queue_name, tick_id, tick_time, self.cur_max_id])

        CascadedConsumer.finish_remote_batch(self, src_db, dst_db, tick_id)

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

