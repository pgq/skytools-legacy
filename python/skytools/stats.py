"""Stats collection.

>>> class PrintSender(LogSender):
...     def output(self, txt):
...         print(txt)
>>> register_sender('print', PrintSender)
>>> config_stats(10, 'print://')
>>> ctx = get_collector('myjob')
>>> ctx.inc('count')
>>> ctx.inc('count')
>>> ctx2 = ctx.get_collector('sub')
>>> ctx2.put('duration', 0.5)
>>> data1 = collect_data()
>>> ctx.inc('count', 2)
>>> ctx2.put('duration', 0.6)
>>> merge_stats(data1)
>>> process_stats(True)
{myjob.count: 4, myjob.sub.duration: 0.55}
"""

import time
import logging
import urlparse
import os.path

__all__ = ['get_collector', 'process_stats', 'merge_stats', 'register_sender',
           'config_stats', 'load_stats_conf']

_start = time.time()
_state = {}

_interval = 30
_sender = None
_prefix = ''

_log_handlers = {}

def register_sender(scheme, sender_class):
    """Register sender class for named schema.
    """
    _log_handlers[scheme] = sender_class

class StatSender(object):
    """Base class for senders.
    """
    def __init__(self, url):
        """url is urlparse() result.
        """
        pass

    def send(self, data):
        """Send stats out.
        """
        pass

class LogSender(StatSender):
    """Print stats to logfile.
    """
    def send(self, data):
        buf = []
        keys = data.keys()
        keys.sort()
        for k in keys:
            v = data[k]
            if isinstance(v, list):
                val = v[0] / v[1]
            else:
                val = v
            buf.append("%s: %s" % (k, val))
        res = "{%s}" % ", ".join(buf)
        self.output(res)

    def output(self, txt):
        logging.info(txt)

register_sender('log', LogSender)

def config_stats(interval, backend):
    """Set up stats backend.
    """
    global _interval, _sender
    _interval = interval
    if backend.find(':') > 0:
        t = urlparse.urlparse(backend)
        if t.scheme in _log_handlers:
            _sender = _log_handlers[t.scheme](t)
        else:
            logging.warning("Unknown stats sender: %s", t.scheme)
    elif backend in _log_handlers:
        _sender = _log_handlers[backend](None)
    else:
        logging.warning("Invalid stats sender: %r", backend)

    if not _sender:
        _sender = LogSender(None)

def load_stats_conf():
    fn = '/etc/stats.ini'
    if os.path.isfile(fn):
        cf = skytools.Config('stats', fn)
        ival = cf.getfloat('interval')
        backend = cf.get('backend')
        config_stats(ival, backend)
    else:
        config_stats(30, 'log')

class StatsContext(object):
    """Position in namespace .
    """
    __slots__ = ['pfx']
    def __init__(self, name):
        if name is None:
            self.pfx = ''
        else:
            self.pfx = name + '.'
    
    def get_collector(self, name):
        """New collector context under this one.
        """
        return StatsContext(self.pfx + name)

    def put(self, name, val):
        """Add a value to be averaged.
        """
        k = self.pfx + name
        v = _state.setdefault(k, [0,0])
        v[0] += val
        v[1] += 1

    def inc(self, name, val=1):
        """Add a value to be summed.
        """
        k = self.pfx + name
        _state[k] = val + _state.get(k, 0)


def get_collector(name):
    """Start up new namespace.
    """
    if name is None:
        return StatsContext(None)
    if _prefix:
        return StatsContext(_prefix + '.' + name)
    return StatsContext(name)

def collect_data():
    """Return current data, resetting it.
    """
    global _state
    _cur_data = _state
    _state = {}
    return _cur_data

def process_stats(force=False):
    """Check if interval is over, then send stats.
    """
    global _start
    now = time.time()
    if now - _start < _interval and not force:
        return
    _start = now

    try:
        _sender.send(collect_data())
    except:
        logging.exception("Problem during stats send")

def merge_stats(data):
    """Merge one stats dict with current one.
    """
    for k, v in data.items():
        if isinstance(v, list):
            s = _state.setdefault(k, [0,0])
            s[0] += v[0]
            s[1] += v[1]
        else:
            _state[k] = v + _state.get(k, 0)

if __name__ == '__main__':
    import doctest
    doctest.testmod()

