"""Our log handlers for Python's logging package.
"""

import sys, os, time, socket
import logging, logging.handlers

from skytools.psycopgwrapper import connect_database
from skytools.quoting import quote_json

_service_name = 'unknown_svc'
def set_service_name(service_name):
    global _service_name
    _service_name = service_name


# configurable file logger
class EasyRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """Easier setup for RotatingFileHandler."""
    def __init__(self, filename, maxBytes = 10*1024*1024, backupCount = 3):
        """Args same as for RotatingFileHandler, but in filename '~' is expanded."""
        fn = os.path.expanduser(filename)
        logging.handlers.RotatingFileHandler.__init__(self, fn, maxBytes=maxBytes, backupCount=backupCount)

# send JSON message over UDP
class UdpLogServerHandler(logging.handlers.DatagramHandler):
    """Sends log records over UDP to logserver in JSON format."""

    # map logging levels to logserver levels
    _level_map = {
        logging.DEBUG   : 'DEBUG',
        logging.INFO    : 'INFO',
        logging.WARNING : 'WARN',
        logging.ERROR   : 'ERROR',
        logging.CRITICAL: 'FATAL',
    }

    # JSON message template
    _log_template = '{\n\t'\
        '"logger": "skytools.UdpLogServer",\n\t'\
        '"timestamp": %.0f,\n\t'\
        '"level": "%s",\n\t'\
        '"thread": null,\n\t'\
        '"message": %s,\n\t'\
        '"properties": {"application":"%s", "apptype": "%s", "type": "sys", "hostname":"%s", "hostaddr": "%s"}\n'\
        '}\n'

    # cut longer msgs
    MAXMSG = 1024

    def makePickle(self, record):
        """Create message in JSON format."""
        # get & cut msg
        msg = self.format(record)
        if len(msg) > self.MAXMSG:
            msg = msg[:self.MAXMSG]
        txt_level = self._level_map.get(record.levelno, "ERROR")
        hostname = socket.gethostname()
        try:
            hostaddr = socket.gethostbyname(hostname)
        except:
            hostaddr = "0.0.0.0"
        jobname = record.name
        svcname = _service_name
        pkt = self._log_template % (time.time()*1000, txt_level, quote_json(msg),
                jobname, svcname, hostname, hostaddr)
        return pkt

    def send(self, s):
        """Disable socket caching."""
        sock = self.makeSocket()
        sock.sendto(s, (self.host, self.port))
        sock.close()

class LogDBHandler(logging.handlers.SocketHandler):
    """Sends log records into PostgreSQL server.

    Additionally, does some statistics aggregating,
    to avoid overloading log server.

    It subclasses SocketHandler to get throtthling for
    failed connections.
    """

    # map codes to string
    _level_map = {
        logging.DEBUG   : 'DEBUG',
        logging.INFO    : 'INFO',
        logging.WARNING : 'WARNING',
        logging.ERROR   : 'ERROR',
        logging.CRITICAL: 'FATAL',
    }

    def __init__(self, connect_string):
        """
        Initializes the handler with a specific connection string.
        """

        logging.handlers.SocketHandler.__init__(self, None, None)
        self.closeOnError = 1

        self.connect_string = connect_string

        self.stat_cache = {}
        self.stat_flush_period = 60
        # send first stat line immidiately
        self.last_stat_flush = 0

    def createSocket(self):
        try:
            logging.handlers.SocketHandler.createSocket(self)
        except:
            self.sock = self.makeSocket()

    def makeSocket(self):
        """Create server connection.
        In this case its not socket but database connection."""

        db = connect_database(self.connect_string)
        db.set_isolation_level(0) # autocommit
        return db

    def emit(self, record):
        """Process log record."""

        # we do not want log debug messages
        if record.levelno < logging.INFO:
            return

        try:
            self.process_rec(record)
        except (SystemExit, KeyboardInterrupt):
            raise
        except:
            self.handleError(record)

    def process_rec(self, record):
        """Aggregate stats if needed, and send to logdb."""
        # render msg
        msg = self.format(record)

        # dont want to send stats too ofter
        if record.levelno == logging.INFO and msg and msg[0] == "{":
            self.aggregate_stats(msg)
            if time.time() - self.last_stat_flush >= self.stat_flush_period:
                self.flush_stats(record.name)
            return

        if record.levelno < logging.INFO:
            self.flush_stats(record.name)

        # dont send more than one line
        ln = msg.find('\n')
        if ln > 0:
            msg = msg[:ln]

        txt_level = self._level_map.get(record.levelno, "ERROR")
        self.send_to_logdb(record.name, txt_level, msg)

    def aggregate_stats(self, msg):
        """Sum stats together, to lessen load on logdb."""

        msg = msg[1:-1]
        for rec in msg.split(", "):
            k, v = rec.split(": ")
            agg = self.stat_cache.get(k, 0)
            if v.find('.') >= 0:
                agg += float(v)
            else:
                agg += int(v)
            self.stat_cache[k] = agg

    def flush_stats(self, service):
        """Send awuired stats to logdb."""
        res = []
        for k, v in self.stat_cache.items():
            res.append("%s: %s" % (k, str(v)))
        if len(res) > 0:
            logmsg = "{%s}" % ", ".join(res)
            self.send_to_logdb(service, "INFO", logmsg)
        self.stat_cache = {}
        self.last_stat_flush = time.time()

    def send_to_logdb(self, service, type, msg):
        """Actual sending is done here."""

        if self.sock is None:
            self.createSocket()
        
        if self.sock:
            logcur = self.sock.cursor()
            query = "select * from log.add(%s, %s, %s)"
            logcur.execute(query, [type, service, msg])

