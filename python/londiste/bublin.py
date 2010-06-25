"""
Experimental event filtering by hash.
"""

import skytools
from londiste.handler import BaseHandler

__all__ = ['Bublin']

class Bublin(BaseHandler):
    plugin_name = 'bublin'

    bubbles_max_slot = None         # NUM_SLOTS - 1 (NUM_SLOTS -> power of 2)
    bubbles_local_slots = None      # dict with local slot numbers

    def __init__(self, name, next, args):
        BaseHandler.__init__(self, name, next, args)
        self.key = args[0]

    def reset(self):
        """Forget config info."""
        if Bublin.bubbles_max_slot:
            Bublin.bubbles_max_slot = None
        if Bublin.bubbles_local_slots:
            Bublin.bubbles_local_slots = None
        BaseHandler.reset(self)

    def add(self, trigger_arg_list):
        """Let trigger put hash into extra3"""

        arg = "ev_extra3='hash='||hashtext(%s)" % skytools.quote_ident(self.key)
        trigger_arg_list.append(arg)

        BaseHandler.add(self, trigger_arg_list)

    def prepare_batch(self, batch_info, dst_curs):
        """Called on first event for this table in current batch."""
        if not self.bubbles_max_slot:
            self.load_bubbles(dst_curs)
        BaseHandler.prepare_batch(self, batch_info, dst_curs)

    def process_event(self, ev, sql_queue_func, arg):
        """Filter event by hash in extra3, apply only local slots."""
        if ev.extra3:
            meta = skytools.db_urldecode(ev.extra3)
            slot = int(meta['hash']) & self.bubbles_max_slot
            if slot not in self.bubbles_local_slots:
                return
        BaseHandler.process_event(self, ev, sql_queue_func, arg)

    def prepare_copy(self, expr_list, dst_curs):
        """Copy only slots needed locally."""
        self.load_bubbles(dst_curs)

        slist = self.bubbles_local_slots.keys()
        fn = 'hashtext(%s)' % skytools.quote_ident(self.key)
        w = "(((%s) & %d) in (%s))" % (fn, self.bubbles_max_slot, slist)
        expr_list.append(w)

        BaseHandler.prepare_copy(self, expr_list, dst_curs)

    def load_bubbles(self, curs):
        """Load slot info from database."""

        q = "select c.max_slot, m.slot_nr from partconf.slot_map m, partconf.conf c"\
            " where c.part_nr = m.part_nr"
        curs.execute(q)
        max_slot = 0
        slot_map = {}
        for row in curs.fetchall():
            if not max_slot:
                max_slot = row['max_slot']
            snr = row['slot_nr']
            slot_map[snr] = 1
        self.bubbles_max_slot = max_slot
        self.bubbles_local_slots = slot_map
        if not max_slot:
            raise Exception("Bubble broke - invalid max_slot")

# register handler class
__londiste_handlers__ = [Bublin]

