"""Our log handlers for Python's logging package.
"""

import os, time, socket
import logging, logging.handlers

import skytools

__all__ = ['getLogger']

# add TRACE level
TRACE = 5
logging.TRACE = TRACE
logging.addLevelName(TRACE, 'TRACE')

# extra info to be added to each log record
_service_name = 'unknown_svc'
_job_name = 'unknown_job'
_hostname = socket.gethostname()
_log_extra = {
    'job_name': _job_name,
    'service_name': _service_name,
    'hostname': _hostname,
}
def set_service_name(service_name, job_name):
    """Set info about current script."""
    global _service_name, _job_name

    _service_name = service_name
    _job_name = job_name

    _log_extra['job_name'] = _job_name
    _log_extra['service_name'] = _service_name

#
# How to make extra fields available to all log records:
# 1. Use own getLogger()
#    - messages logged otherwise (eg. from some libs)
#      will crash the logging.
# 2. Fix record in own handlers
#    - works only with custom handlers, standard handlers will
#      crash is used with custom fmt string.
# 3. Change root logger
#    - can't do it after non-root loggers are initialized,
#      doing it before will depend on import order.
# 4. Update LogRecord.__dict__
#    - fails, as formatter uses obj.__dict__ directly.
# 5. Change LogRecord class
#    - ugly but seems to work.
#
_OldLogRecord = logging.LogRecord
class _NewLogRecord(_OldLogRecord):
    def __init__(self, *args):
        _OldLogRecord.__init__(self, *args)
        self.__dict__.update(_log_extra)
logging.LogRecord = _NewLogRecord


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
        hostname = _hostname
        try:
            hostaddr = socket.gethostbyname(hostname)
        except:
            hostaddr = "0.0.0.0"
        jobname = _job_name
        svcname = _service_name
        pkt = self._log_template % (time.time()*1000, txt_level, skytools.quote_json(msg),
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

        db = skytools.connect_database(self.connect_string)
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
                self.flush_stats(_job_name)
            return

        if record.levelno < logging.INFO:
            self.flush_stats(_job_name)

        # dont send more than one line
        ln = msg.find('\n')
        if ln > 0:
            msg = msg[:ln]

        txt_level = self._level_map.get(record.levelno, "ERROR")
        self.send_to_logdb(_job_name, txt_level, msg)

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
        """Send acquired stats to logdb."""
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

# fix unicode bug in SysLogHandler
class SysLogHandler(logging.handlers.SysLogHandler):
    """Fixes unicode bug in logging.handlers.SysLogHandler."""

    # be compatible with both 2.6 and 2.7
    socktype = socket.SOCK_DGRAM

    _udp_reset = 0

    def _custom_format(self, record):
        msg = self.format(record) + '\000'
        """
        We need to convert record level to lowercase, maybe this will
        change in the future.
        """
        prio = '<%d>' % self.encodePriority(self.facility,
                                            self.mapPriority(record.levelname))
        msg = prio + msg
        return msg

    def emit(self, record):
        """
        Emit a record.

        The record is formatted, and then sent to the syslog server. If
        exception information is present, it is NOT sent to the server.
        """
        msg = self._custom_format(record)
        # Message is a string. Convert to bytes as required by RFC 5424
        if type(msg) is unicode:
            msg = msg.encode('utf-8')
            ## this puts BOM in wrong place
            #if codecs:
            #    msg = codecs.BOM_UTF8 + msg
        try:
            if self.unixsocket:
                try:
                    self.socket.send(msg)
                except socket.error:
                    self._connect_unixsocket(self.address)
                    self.socket.send(msg)
            elif self.socktype == socket.SOCK_DGRAM:
                now = time.time()
                if now - 1 > self._udp_reset:
                    self.socket.close()
                    self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    self._udp_reset = now
                self.socket.sendto(msg, self.address)
            else:
                self.socket.sendall(msg)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

class SysLogHostnameHandler(SysLogHandler):
    """Slightly modified standard SysLogHandler - sends also hostname and service type"""

    def _custom_format(self, record):
        msg = self.format(record)
        format_string = '<%d> %s %s %s\000'
        msg = format_string % (self.encodePriority(self.facility,self.mapPriority(record.levelname)),
                               _hostname,
                               _service_name,
                               msg)
        return msg

try:
    from logging import LoggerAdapter
except ImportError:
    # LoggerAdapter is missing from python 2.5
    class LoggerAdapter(object):
        def __init__(self, logger, extra):
            self.logger = logger
            self.extra = extra
        def process(self, msg, kwargs):
            kwargs["extra"] = self.extra
            return msg, kwargs
        def debug(self, msg, *args, **kwargs):
            msg, kwargs = self.process(msg, kwargs)
            self.logger.debug(msg, *args, **kwargs)
        def info(self, msg, *args, **kwargs):
            msg, kwargs = self.process(msg, kwargs)
            self.logger.info(msg, *args, **kwargs)
        def warning(self, msg, *args, **kwargs):
            msg, kwargs = self.process(msg, kwargs)
            self.logger.warning(msg, *args, **kwargs)
        def error(self, msg, *args, **kwargs):
            msg, kwargs = self.process(msg, kwargs)
            self.logger.error(msg, *args, **kwargs)
        def exception(self, msg, *args, **kwargs):
            msg, kwargs = self.process(msg, kwargs)
            kwargs["exc_info"] = 1
            self.logger.error(msg, *args, **kwargs)
        def critical(self, msg, *args, **kwargs):
            msg, kwargs = self.process(msg, kwargs)
            self.logger.critical(msg, *args, **kwargs)
        def log(self, level, msg, *args, **kwargs):
            msg, kwargs = self.process(msg, kwargs)
            self.logger.log(level, msg, *args, **kwargs)

# add missing aliases (that are in Logger class)
LoggerAdapter.fatal = LoggerAdapter.critical
LoggerAdapter.warn = LoggerAdapter.warning

class SkyLogger(LoggerAdapter):
    def __init__(self, logger, extra):
        LoggerAdapter.__init__(self, logger, extra)
        self.name = logger.name
    def trace(self, msg, *args, **kwargs):
        """Log 'msg % args' with severity 'TRACE'."""
        self.log(TRACE, msg, *args, **kwargs)
    def addHandler(self, hdlr):
        """Add the specified handler to this logger."""
        self.logger.addHandler(hdlr)
    def isEnabledFor(self, level):
        """See if the underlying logger is enabled for the specified level."""
        return self.logger.isEnabledFor(level)

def getLogger(name=None, **kwargs_extra):
    """Get logger with extra functionality.

    Adds additional log levels, and extra fields to log record.

    name - name for logging.getLogger()
    kwargs_extra - extra fields to add to log record
    """
    log = logging.getLogger(name)
    return SkyLogger(log, kwargs_extra)
