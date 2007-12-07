#! /usr/bin/env python

import sys, time, skytools

from pgq.rawconsumer import RawQueue

__all__ = ['SetConsumer']

ROOT = 'root'
BRANCH = 'branch'
LEAF = 'leaf'
COMBINED_ROOT = 'combined-root'
COMBINED_BRANCH = 'combined-branch'
MERGE_LEAF = 'merge-leaf'

class MemberInfo:
    def __init__(self, row):
        self.name = row['node_name']
        self.location = row['node_location']
        self.dead = row['dead']

class NodeInfo:
    def __init__(self, row, member_list):
        self.member_map = {}
        for r in member_list:
            m = MemberInfo(r)
            self.member_map[m.name] = m

        self.name = row['node_name']
        self.type = row['node_type']
        self.queue_name = row['queue_name']
        self.global_watermark = row['global_watermark']
        self.local_watermark = row['local_watermark']
        self.completed_tick = row['completed_tick']
        self.provider_node = row['provider_node']
        self.provider_location = row['provider_location']
        self.paused = row['paused']
        self.resync = row['resync']
        self.up_to_date = row['up_to_date']
        self.combined_set = row['combined_set']
        self.combined_type = row['combined_type']
        self.combined_queue = row['combined_queue']
        self.worker_name = row['worker_name']

    def need_action(self, action_name):
        typ = self.type
        if type == 'merge-leaf':
            if self.target_type == 'combined-branch':
                typ += "merge-leaf-to-branch"
            elif self.target_type == 'combined-root':
                typ += "merge-leaf-to-root"
            else:
                raise Exception('bad target type')

        try:
            return action_map[action_name][typ]
        except KeyError, d:
            raise Exception('need_action(name=%s, type=%s) unknown' % (action_name, typ))

    def get_target_queue(self):
        qname = None
        if self.type == 'merge-leaf':
            qname = self.combined_queue
        else:
            qname = self.queue_name
        if qname is None:
            raise Exception("no target queue")
        return qname

action_map = {
'process-batch':   {'root':0, 'branch':1, 'leaf':1, 'combined-root':0, 'combined-branch':1, 'merge-leaf-to-root':1, 'merge-leaf-to-branch':1},
'process-events':  {'root':0, 'branch':1, 'leaf':0, 'combined-root':0, 'combined-branch':1, 'merge-leaf-to-root':1, 'merge-leaf-to-branch':0},
'copy-events':     {'root':0, 'branch':1, 'leaf':1, 'combined-root':0, 'combined-branch':1, 'merge-leaf-to-root':0, 'merge-leaf-to-branch':0},
'tick-event':      {'root':0, 'branch':0, 'leaf':0, 'combined-root':0, 'combined-branch':0, 'merge-leaf-to-root':1, 'merge-leaf-to-branch':0},
'global-wm-event': {'root':1, 'branch':0, 'leaf':0, 'combined-root':1, 'combined-branch':0, 'merge-leaf-to-root':0, 'merge-leaf-to-branch':0},
'wait-behind':     {'root':0, 'branch':0, 'leaf':0, 'combined-root':0, 'combined-branch':0, 'merge-leaf-to-root':0, 'merge-leaf-to-branch':1},
'sync-part-pos':   {'root':0, 'branch':0, 'leaf':0, 'combined-root':0, 'combined-branch':1, 'merge-leaf-to-root':0, 'merge-leaf-to-branch':0},
}

node_properties = {
'pgq':     {'root':1, 'branch':1, 'leaf':0, 'combined-root':1, 'combined-branch':1, 'merge-leaf':1},
'queue':   {'root':1, 'branch':1, 'leaf':0, 'combined-root':1, 'combined-branch':1, 'merge-leaf':0},
}

class SetConsumer(skytools.DBScript):
    last_global_wm_event = 0
    def work(self):


        self.tick_id_cache = {}

        self.set_name = self.cf.get('set_name')
        target_db = self.get_database('subscriber_db')

        node = self.load_node_info(target_db)
        self.consumer_name = node.worker_name

        if not node.up_to_date:
            self.tag_node_uptodate(target_db)

        if node.paused:
            return 0

        if node.need_action('global-wm-event'):
            curs = target_db.cursor()
            self.set_global_watermark(curs, node.local_watermark)
            target_db.commit()

        if not node.need_action('process-batch'):
            return 0

        #
        # batch processing follows
        #

        source_db = self.get_database('source_db', connstr = node.provider_location)
        srcnode = self.load_node_info(source_db)
        
        # get batch
        srcqueue = RawQueue(srcnode.queue_name, self.consumer_name)

        batch_id = srcqueue.next_batch(source_db.cursor())
        source_db.commit()
        if batch_id is None:
            return 0

        if node.need_action('wait-behind'):
            if node.should_wait(queue.cur_tick):
                return 0

        if node.need_action('process-event'):
            # load and process batch data
            ev_list = self.get_batch_events(source_db, batch_id)

            copy_queue = None
            if node.need_action('copy-events'):
                copy_queue = node.get_target_queue()
            self.process_set_batch(target_db, ev_list, copy_queue)
            if copy_queue:
                copy_queue.finish_bulk_insert(curs)
                self.copy_tick(target_curs, srcqueue, copy_queue)

            # COMBINED_BRANCH needs to sync with part sets
            if node.need_action('sync-part-pos'):
                self.move_part_positions(target_curs)

        # we are done on target
        self.set_tick_complete(target_curs)
        target_db.commit()

        # done on source
        self.finish_batch(source_db, batch_id)

        # occasinally send watermark upwards
        self.send_local_watermark_upwards(target_db, source_db)

        # got a batch so there can be more
        return 1

    def process_set_batch(self, src_db, dst_db, ev_list, copy_queue = None):
        curs = db.cursor()
        for ev in ev_list:
            self.process_set_event(curs, ev)
            if copy_queue:
                copy_queue.bulk_insert(curs, ev)
        self.stat_add('count', len(ev_list))

    def process_set_event(self, curs, ev):
        if ev.type == 'set-tick':
            self.handle_set_tick(curs, ev)
        elif ev.type == 'set-member-info':
            self.handle_member_info(curs, ev)
        elif ev.type == 'global-watermark':
            self.handle_global_watermark(curs, ev)
        else:
            raise Exception('bad event for set consumer')

    def handle_global_watermark(self, curs, ev):
        set_name = ev.extra1
        tick_id = ev.data
        if set_name == self.set_name:
            self.set_global_watermark(curs, tick_id)

    def handle_set_tick(self, curs, ev):
        data = skytools.db_urldecode(ev.data)
        set_name = data['set_name']
        tick_id = data['tick_id']
        self.tick_id_cache[set_name] = tick_id

    def move_part_positions(self, curs):
        q = "select * from pgq_set.set_partition_watermark(%s, %s, %s)"
        for set_name, tick_id in self.tick_id_cache.items():
            curs.execute(q, [self.set_name, set_name, tick_id])

    def handle_member_info(self, curs, ev):
        data = skytools.db_urldecode(ev.data)
        set_name = data['set_name']
        node_name = data['node_name']
        node_location = data['node_location']
        dead = data['dead']
        # this can also be member for part set, ignore then
        if set_name != self.set_name:
            return

        q = "select * from pgq_set.add_member(%s, %s, %s, %s)"
        curs.execute(q, [set_name, node_name, node_location, dead])

    def send_local_watermark_upwards(self, target_db, source_db):
        target_curs = target_db.cursor()
        source_curs = source_db.cursor()
        q = "select pgq_ext.get_local_watermark(%s)"
        target_curs.execute(q, [self.set_name])
        wm = target_curs.fetchone()[0]
        target_db.commit()
    
        q = "select pgq_ext.set_subscriber_watermark(%s, %s, %s)"
        source_curs.execute(q, [self.set_name])

    def set_global_watermark(self, curs, tick_id):
        q = "select pgq_set.set_global_watermark(%s, %s)"
        curs.execute(q, [self.set_name, tick_id])

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

        return NodeInfo(node_row, mbr_list)

    def tag_node_uptodate(self, db):
        curs = db.cursor()
        q = "select * from pgq_set.set_node_uptodate(%s, true)"
        curs.execute(q, [self.set_name])
        db.commit()

    def copy_tick(self, curs, src_queue, dst_queue):
        q = "select * from pgq.ticker(%s, %s)"
        curs.execute(q, [dst_queue.queue_name, src_queue.cur_tick])

if __name__ == '__main__':
    script = SetConsumer('setconsumer', sys.argv[1:])
    script.start()

