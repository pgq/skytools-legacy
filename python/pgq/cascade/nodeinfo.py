#! /usr/bin/env python

"""Info about node/set/members.  For admin tool.
"""

__all__ = ['MemberInfo', 'NodeInfo', 'QueueInfo']

# node types
ROOT = 'root'
BRANCH = 'branch'
LEAF = 'leaf'

class MemberInfo:
    """Info about set member."""
    def __init__(self, row):
        self.name = row['node_name']
        self.location = row['node_location']
        self.dead = row['dead']

class NodeInfo:
    """Detailed info about set node."""
    def __init__(self, queue_name, row, main_worker = True):
        self.queue_name = queue_name
        self.member_map = {}
        self.main_worker = main_worker

        self.name = row['node_name']
        self.type = row['node_type']
        self.global_watermark = row['global_watermark']
        self.local_watermark = row['local_watermark']
        self.completed_tick = row['worker_last_tick']
        self.provider_node = row['provider_node']
        self.provider_location = row['provider_location']
        self.consumer_name = row['worker_name']
        self.worker_name = row['worker_name']
        self.paused = row['worker_paused']
        self.uptodate = row['worker_uptodate']
        self.combined_queue = row['combined_queue']
        self.combined_type = row['combined_type']

        self.parent = None
        self.consumer_map = {}
        self.queue_info = {}

        self._row = row

        self._info_lines = []

    def __get_target_queue(self):
        qname = None
        if self.type == LEAF:
            if self.combined_queue:
                qname = self.combined_queue
            else:
                return None
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
            tick_time = self.parent.consumer_map[self.consumer_name]['tick_time']
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
            curs.execute(q, [self.queue_name])
            for row in curs.fetchall():
                cname = row['consumer_name']
                self.consumer_map[cname] = row
            q = "select current_timestamp - ticker_lag as tick_time,"\
                "  ticker_lag, current_timestamp as now "\
                "from pgq.get_queue_info(%s)"
            curs.execute(q, [self.queue_name])
            self.queue_info = curs.fetchone()

class QueueInfo:
    """Info about cascaded queue.
    
    Slightly broken, as all info is per-node.
    """

    def __init__(self, queue_name, info_row, member_rows):
        self.local_node = NodeInfo(queue_name, info_row)
        self.queue_name = queue_name
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

    #
    # Rest is about printing the tree
    #

    _DATAFMT = "%-30s%s"
    def print_tree(self):
        """Print ascii-tree for set.
        Expects that data for all nodes is filled in."""

        root = self._prepare_tree()
        self._tree_calc(root)
        datalines = self._print_node(root, '', [])
        for ln in datalines:
            print(self._DATAFMT % (' ', ln))

    def _print_node(self, node, pfx, datalines):
        # print a tree fragment for node and info
        # returns list of unprinted data rows
        for ln in datalines:
            print(self._DATAFMT % (_setpfx(pfx, '|'), ln))
        datalines = node.get_infolines()
        print("%s%s" % (_setpfx(pfx, '+--'), node.name))

        for i, n in enumerate(node.child_list):
            sfx = ((i < len(node.child_list) - 1) and '  |' or '   ')
            datalines = self._print_node(n, pfx + sfx, datalines)

        return datalines

    def _prepare_tree(self):
        # reset vars, fill parent and child_list for each node
        # returns root
        root = None
        for node in self.node_map.values():
            node.total_childs = 0
            node.levels = 0
            node.child_list = []
            if node.type == ROOT:
                root = node
        for node in self.node_map.values():
            if node.provider_node and node.provider_node != node.name:
                p = self.node_map[node.provider_node]
                p.child_list.append(node)
                node.parent = p
            else:
                node.parent = None

        if root is None:
            raise Exception("root node not found")
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
        node.child_list.sort(key = _node_key)

def _setpfx(pfx, sfx):
    if pfx:
        pfx = pfx[:-1] + sfx
    return pfx

def _node_key(n):
    return (n.levels, n.total_childs)

