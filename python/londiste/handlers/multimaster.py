#!/usr/bin/env python
# encoding: utf-8
"""
Handler for replica with multiple master nodes.

Can only handle initial copy from one master. Add other masters with
expect-sync option.

NB! needs merge_on_time function to be compiled on database first.
"""

import skytools
from londiste.handlers.applyfn import ApplyFuncHandler
from londiste.handlers import update

__all__ = ['MultimasterHandler']

class MultimasterHandler(ApplyFuncHandler):
    __doc__ = __doc__
    handler_name = 'multimaster'

    def __init__(self, table_name, args, dest_table):
        """Init per-batch table data cache."""
        conf = args.copy()
        # remove Multimaster args from conf
        for name in ['func_name','func_conf']:
            if name in conf:
                conf.pop(name)
        conf = skytools.db_urlencode(conf)
        args = update(args, {'func_name': 'merge_on_time', 'func_conf': conf})
        ApplyFuncHandler.__init__(self, table_name, args, dest_table)

    def _check_args (self, args):
        pass # any arg can be passed

    def add(self, trigger_arg_list):
        """Create SKIP and BEFORE INSERT trigger"""
        trigger_arg_list.append('no_merge')


#------------------------------------------------------------------------------
# register handler class
#------------------------------------------------------------------------------

__londiste_handlers__ = [MultimasterHandler]
