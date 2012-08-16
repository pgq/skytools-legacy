"""Virtual Table.
"""

from londiste.handler import BaseHandler

__all__ = ['VirtualTableHandler', 'FakeLocalHandler']

class VirtualTableHandler(BaseHandler):
    """Virtual Table.

    Hack to get local=t for a table, but without processing any events.
    """
    handler_name = 'vtable'

    def add(self, trigger_arg_list):
        trigger_arg_list.append('virtual_table')

    def needs_table(self):
        return False

class FakeLocalHandler(VirtualTableHandler):
    """Deprecated compat name for vtable."""
    handler_name = 'fake_local'

__londiste_handlers__ = [VirtualTableHandler, FakeLocalHandler]

