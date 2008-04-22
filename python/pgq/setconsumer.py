#! /usr/bin/env python

import sys, time, skytools

from pgq.rawconsumer import RawQueue
from pgq.setinfo import *

__all__ = ['SetConsumer']

class SetConsumer(skytools.DBScript):
    last_local_wm_publish_time = 0
    last_global_wm_publish_time = 0
    main_worker = True
    reg_ok = False
    def __init__(self, service_name, args,
                 node_db_name = 'node_db'):
        skytools.DBScript.__init__(self, service_name, args)
        self.node_db_name = node_db_name
        self.consumer_name = self.cf.get('consumer_name', self.job_name)

    def work(self):
        self.tick_id_cache = {}

        self.set_name = self.cf.get('set_name')
        dst_db = self.get_database(self.node_db_name)
        dst_curs = dst_db.cursor()

        dst_node = self.load_node_info(dst_db)
        if self.main_worker:
            self.consumer_name = dst_node.worker_name
            if not dst_node.up_to_date:
                self.tag_node_uptodate(dst_db)

        if dst_node.paused:
            return 0

        if dst_node.need_action('global-wm-event'):
            self.publish_global_watermark(dst_db, dst_node.local_watermark)

        if not dst_node.need_action('process-batch'):
            return 0

        #
        # batch processing follows
        #

        src_db = self.get_database('src_db', connstr = dst_node.provider_location)
        src_curs = src_db.cursor()
        src_node = self.load_node_info(src_db)
        
        # get batch
        src_queue = RawQueue(src_node.queue_name, self.consumer_name)
        self.src_queue = src_queue
        self.dst_queue = None

        if not self.main_worker and not self.reg_ok:
            self.register_consumer(src_curs)

        batch_id = src_queue.next_batch(src_curs)
        src_db.commit()
        if batch_id is None:
            return 0

        self.log.debug("New batch: tick_id=%d / batch_id=%d" % (src_queue.cur_tick, batch_id))

        if dst_node.need_action('wait-behind'):
            if dst_node.should_wait(src_queue.cur_tick):
                return 0

        if dst_node.need_action('process-events'):
            # load and process batch data
            ev_list = src_queue.get_batch_events(src_curs)

            if dst_node.need_action('copy-events'):
                self.dst_queue = RawQueue(dst_node.get_target_queue(), self.consumer_name)
            self.process_set_batch(src_db, dst_db, ev_list)
            if self.dst_queue:
                self.dst_queue.finish_bulk_insert(dst_curs)
                self.copy_tick(dst_curs, src_queue, self.dst_queue)

            # COMBINED_BRANCH needs to sync with part sets
            if dst_node.need_action('sync-part-pos'):
                self.move_part_positions(dst_curs)

        # we are done on target
        self.set_tick_complete(dst_curs, src_queue.cur_tick)
        dst_db.commit()

        # done on source
        src_queue.finish_batch(src_curs)
        src_db.commit()

        # occasinally send watermark upwards
        if dst_node.need_action('local-wm-publish'):
            self.send_local_watermark_upwards(src_db, dst_node)

        # got a batch so there can be more
        return 1

    def process_set_batch(self, src_db, dst_db, ev_list):
        dst_curs = dst_db.cursor()
        for ev in ev_list:
            self.process_set_event(dst_curs, ev)
            if self.dst_queue:
                self.dst_queue.bulk_insert(dst_curs, ev)
        self.stat_increase('count', len(ev_list))

    def process_set_event(self, dst_curs, ev):
        if ev.type == 'set-tick':
            self.handle_set_tick(dst_curs, ev)
        elif ev.type == 'set-member-info':
            self.handle_member_info(dst_curs, ev)
        elif ev.type == 'global-watermark':
            self.handle_global_watermark(dst_curs, ev)
        else:
            raise Exception('bad event for set consumer')

    def handle_global_watermark(self, dst_curs, ev):
        set_name = ev.extra1
        tick_id = ev.data
        if set_name == self.set_name:
            self.set_global_watermark(dst_curs, tick_id)

    def handle_set_tick(self, dst_curs, ev):
        data = skytools.db_urldecode(ev.data)
        set_name = data['set_name']
        tick_id = data['tick_id']
        self.tick_id_cache[set_name] = tick_id

    def move_part_positions(self, dst_curs):
        q = "select * from pgq_set.set_partition_watermark(%s, %s, %s)"
        for set_name, tick_id in self.tick_id_cache.items():
            dst_curs.execute(q, [self.set_name, set_name, tick_id])

    def handle_member_info(self, dst_curs, ev):
        data = skytools.db_urldecode(ev.data)
        set_name = data['set_name']
        node_name = data['node_name']
        node_location = data['node_location']
        dead = data['dead']
        # this can also be member for part set, ignore then
        if set_name != self.set_name:
            return

        q = "select * from pgq_set.add_member(%s, %s, %s, %s)"
        dst_curs.execute(q, [set_name, node_name, node_location, dead])

    def send_local_watermark_upwards(self, src_db, node):
        # fixme - delay
        now = time.time()
        delay = now - self.last_local_wm_publish_time
        if delay < 1*60:
            return
        self.last_local_wm_publish_time = now

        self.log.debug("send_local_watermark_upwards")
        src_curs = src_db.cursor()
        q = "select pgq_set.set_subscriber_watermark(%s, %s, %s)"
        src_curs.execute(q, [self.set_name, node.name, node.local_watermark])
        src_db.commit()

    def set_global_watermark(self, dst_curs, tick_id):
        self.log.debug("set_global_watermark: %s" % tick_id)
        q = "select pgq_set.set_global_watermark(%s, %s)"
        dst_curs.execute(q, [self.set_name, tick_id])

    def publish_global_watermark(self, dst_db, watermark):
        now = time.time()
        delay = now - self.last_global_wm_publish_time
        if delay < 1*60:
            return
        self.last_global_wm_publish_time = now

        self.set_global_watermark(dst_db.cursor(), watermark)
        dst_db.commit()

    def load_node_info(self, db):
        curs = db.cursor()

        q = "select * from pgq_set.get_node_info(%s)"
        curs.execute(q, [self.set_name])
        node_row = curs.dictfetchone()
        if not node_row:
            raise Exception('node not initialized')

        q = "select * from pgq_set.get_member_info(%s)"
        curs.execute(q, [self.set_name])
        mbr_list = curs.dictfetchall()
        db.commit()

        return NodeInfo(node_row, self.main_worker)

    def tag_node_uptodate(self, dst_db):
        dst_curs = dst_db.cursor()
        q = "select * from pgq_set.set_node_uptodate(%s, true)"
        dst_curs.execute(q, [self.set_name])
        dst_db.commit()

    def copy_tick(self, dst_curs, src_queue, dst_queue):
        q = "select * from pgq.ticker(%s, %s)"
        dst_curs.execute(q, [dst_queue.queue_name, src_queue.cur_tick])

    def set_tick_complete(self, dst_curs, tick_id):
        q = "select * from pgq_set.set_completed_tick(%s, %s, %s)"
        dst_curs.execute(q, [self.set_name, self.consumer_name, tick_id])

    def register_consumer(self, src_curs):
        if self.main_worker:
            raise Exception('main set worker should not play with registrations')

        q = "select * from pgq.register_consumer(%s, %s)"
        src_curs.execute(q, [self.src_queue.queue_name, self.consumer_name])

    def unregister_consumer(self, src_curs):
        if self.main_worker:
            raise Exception('main set worker should not play with registrations')

        q = "select * from pgq.unregister_consumer(%s, %s)"
        src_curs.execute(q, [self.src_queue.queue_name, self.consumer_name])

if __name__ == '__main__':
    script = SetConsumer('setconsumer', sys.argv[1:])
    script.start()

