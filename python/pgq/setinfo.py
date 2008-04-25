#! /usr/bin/env python

__all__ = ['MemberInfo', 'NodeInfo', 'SetInfo',
        'ROOT', 'BRANCH', 'LEAF', 'COMBINED_ROOT',
        'COMBINED_BRANCH', 'MERGE_LEAF']

# node types
ROOT = 'root'
BRANCH = 'branch'
LEAF = 'leaf'
COMBINED_ROOT = 'combined-root'
COMBINED_BRANCH = 'combined-branch'
MERGE_LEAF = 'merge-leaf'

# which nodes need to do what actions
action_map = {
'process-batch':   {'root':0, 'branch':1, 'leaf':1, 'combined-root':0, 'combined-branch':1, 'merge-leaf-to-root':1, 'merge-leaf-to-branch':1},
'process-events':  {'root':0, 'branch':1, 'leaf':1, 'combined-root':0, 'combined-branch':1, 'merge-leaf-to-root':1, 'merge-leaf-to-branch':0},
'copy-events':     {'root':0, 'branch':1, 'leaf':0, 'combined-root':0, 'combined-branch':1, 'merge-leaf-to-root':0, 'merge-leaf-to-branch':0},
'tick-event':      {'root':0, 'branch':0, 'leaf':0, 'combined-root':0, 'combined-branch':0, 'merge-leaf-to-root':1, 'merge-leaf-to-branch':0},
'global-wm-event': {'root':1, 'branch':0, 'leaf':0, 'combined-root':1, 'combined-branch':0, 'merge-leaf-to-root':0, 'merge-leaf-to-branch':0},
'wait-behind':     {'root':0, 'branch':0, 'leaf':0, 'combined-root':0, 'combined-branch':0, 'merge-leaf-to-root':0, 'merge-leaf-to-branch':1},
'sync-part-pos':   {'root':0, 'branch':0, 'leaf':0, 'combined-root':0, 'combined-branch':1, 'merge-leaf-to-root':0, 'merge-leaf-to-branch':0},
'local-wm-publish':{'root':0, 'branch':1, 'leaf':1, 'combined-root':0, 'combined-branch':1, 'merge-leaf-to-root':1, 'merge-leaf-to-branch':1},
}

class MemberInfo:
    def __init__(self, row):
        self.name = row['node_name']
        self.location = row['node_location']
        self.dead = row['dead']

class NodeInfo:
    def __init__(self, set_name, row, main_worker = True):
        self.set_name = set_name
        self.member_map = {}
        self.main_worker = main_worker

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
        self.uptodate = row['uptodate']
        self.combined_set = row['combined_set']
        self.combined_type = row['combined_type']
        self.combined_queue = row['combined_queue']

        self._row = row

        self._info_lines = []

    def need_action(self, action_name):
        """Returns True if worker for this node needs
        to do specified action.
        """
        if not self.main_worker:
            return action_name in ('process-batch', 'process-events')

        typ = self.type
        if type == MERGE_LEAF:
            if self.target_type == COMBINED_BRANCH:
                typ = "merge-leaf-to-branch"
            elif self.target_type == COMBINED_ROOT:
                typ = "merge-leaf-to-root"
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

    def get_infolines(self):
        lst = self._info_lines
        if self.parent:
            root = self.parent
            while root.parent:
                root = root.parent
            tick_time = self.parent.consumer_map[self.name]['tick_time']
            root_time = root.queue_info['now']
            lag = root_time - tick_time
        else:
            lag = self.queue_info['ticker_lag']
        txt = "lag: %s" % lag
        if self.paused:
            txt += ", PAUSED"
        if not self.uptodate:
            txt += ", NOT UPTODATE"
        lst.append(txt)
        return lst
    
    def add_info_line(self, ln):
        self._info_lines.append(ln)

    def load_status(self, curs):
        self.consumer_map = {}
        self.queue_info = {}
        if self.queue_name:
            q = "select consumer_name, current_timestamp - lag as tick_time,"\
                "  lag, last_seen, last_tick "\
                "from pgq.get_consumer_info(%s)"
            curs.execute(q, [self.set_name])
            for row in curs.fetchall():
                cname = row['consumer_name']
                self.consumer_map[cname] = row
            q = "select current_timestamp - ticker_lag as tick_time,"\
                "  ticker_lag, current_timestamp as now "\
                "from pgq.get_queue_info(%s)"
            curs.execute(q, [self.set_name])
            self.queue_info = curs.fetchone()

class SetInfo:
    def __init__(self, set_name, info_row, member_rows):
        self.local_node = NodeInfo(set_name, info_row)
        self.set_name = set_name
        self.member_map = {}
        self.node_map = {}
        self.add_node(self.local_node)

        for r in member_rows:
            n = MemberInfo(r)
            self.member_map[n.name] = n

    def get_member(self, name):
        return self.member_map.get(name)

    def get_node(self, name):
        return self.node_map.get(name)

    def add_node(self, node):
        self.node_map[node.name] = node

    _DATAFMT = "%-30s%s"
    def print_tree(self):
        """Print ascii-tree for set.
        Expects that data for all nodes is filled in."""

        root = self._prepare_tree()
        self._tree_calc(root)
        datalines = self._print_node(root, '', [])
        for ln in datalines:
            print self._DATAFMT % (' ', ln)

    def _print_node(self, node, pfx, datalines):
        # print a tree fragment for node and info
        # returns list of unprinted data rows
        for ln in datalines:
            print self._DATAFMT % (_setpfx(pfx, '|'), ln)
        datalines = node.get_infolines()
        print "%s%s" % (_setpfx(pfx, '+--'), node.name)

        for i, n in enumerate(node.child_list):
            sfx = ((i < len(node.child_list) - 1) and '  |' or '   ')
            datalines = self._print_node(n, pfx + sfx, datalines)

        return datalines

    def _prepare_tree(self):
        # reset vars, fill parent and child_list for each node
        # returns root
        root = None
        for node in self.node_map.itervalues():
            node.total_childs = 0
            node.levels = 0
            node.child_list = []
            if node.type in (ROOT, COMBINED_ROOT):
                root = node
        for node in self.node_map.itervalues():
            if node.provider_node:
                p = self.node_map[node.provider_node]
                p.child_list.append(node)
                node.parent = p
            else:
                node.parent = None

        if root is None:
            raise Exception("root nod enot found")
        return root

    def _tree_calc(self, node):
        # calculate levels and count total childs
        # sort the tree based on them
        total = len(node.child_list)
        levels = 1
        for subnode in node.child_list:
            self._tree_calc(subnode)
            total += subnode.total_childs
            if levels < subnode.levels + 1:
                levels = subnode.levels + 1
        node.total_childs = total
        node.levels = levels
        node.child_list.sort(_cmp_node)

def _setpfx(pfx, sfx):
    if pfx:
        pfx = pfx[:-1] + sfx
    return pfx


def _cmp_node(n1, n2):
    # returns neg if n1 smaller
    cmp = n1.levels - n2.levels
    if cmp == 0:
        cmp = n1.total_childs - n2.total_childs
    return cmp

