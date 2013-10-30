r"""Send stats to Carbon listener.

>>> class TestCarbon(SendCarbon):
...     def send(self, data):
...         print("[%r] %r" % (self.addr, self.render(data, 10000)))
>>> stats.register_sender('xcarbon', TestCarbon)
>>> stats.config_stats(10, 'xcarbon://host:999/xapp/test')
>>> ctx = stats.get_collector('myapp')
>>> ctx.inc('count')
>>> stats.process_stats(True)
[('host', 999)] 'xapp.test.myapp.count 1 10000\n'
"""

from skytools import stats
import time
import socket

__all__ = []

class SendCarbon(stats.StatSender):
    def __init__(self, ut):
        super(SendCarbon, self).__init__(ut)

        # calc prefix
        self.prefix = ut.path.replace('/', '.').strip('.')
        if self.prefix:
            self.prefix += '.'

        # calc address
        a = ut.netloc.split(':', 1)
        if len(a) == 2:
            self.addr = (a[0], int(a[1]))
        elif a[0]:
            self.addr = (a[0], 2003)
        else:
            self.addr = ('127.0.0.1', 2003)

    def send(self, data):
        now = int(time.time())
        sk = None
        try:
            sk = socket.create_connection(self.addr, 5)
            pkt = self.render(data, now)
            sk.send(pkt)
        finally:
            if sk:
                sk.close()

    def render(self, data, now):
        buf = []
        for k, v in data.items():
            if isinstance(v, list):
                x = v[0] / v[1]
                val = str(x)
            else:
                val = str(v)
            ln = "%s%s %s %d\n" % (self.prefix, k, val, now)
            buf.append(ln)
        return "".join(buf)

stats.register_sender('carbon', SendCarbon)

if __name__ == '__main__':
    import doctest
    doctest.testmod()

