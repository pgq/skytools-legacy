
"""Useful functions and classes for database scripts."""

import sys, os, signal, optparse, time, errno
import logging, logging.handlers, logging.config

from skytools.config import *
from skytools.psycopgwrapper import connect_database
from skytools.quoting import quote_statement
import skytools.skylog, psycopg2

__pychecker__ = 'no-badexcept'

#: how old connections need to be closed
DEF_CONN_AGE = 20*60  # 20 min

#: isolation level not set
I_DEFAULT = -1

#: isolation level constant for AUTOCOMMIT
I_AUTOCOMMIT = 0
#: isolation level constant for READ COMMITTED
I_READ_COMMITTED = 1
#: isolation level constant for SERIALIZABLE
I_SERIALIZABLE = 2

__all__ = ['DBScript', 'I_AUTOCOMMIT', 'I_READ_COMMITTED', 'I_SERIALIZABLE',
           'signal_pidfile', 'UsageError']
#__all__ += ['daemonize', 'run_single_process']

class UsageError(Exception):
    """User induced error."""
    pass

#
# utils
#

def signal_pidfile(pidfile, sig):
    """Send a signal to process whose ID is located in pidfile.

    Read only first line of pidfile to support multiline
    pifiles like postmaster.pid.

    Returns True is successful, False if pidfile does not exist
    or process itself is dead.  Any other errors will passed
    as exceptions.
    """
    try:
        pid = int(open(pidfile, 'r').readline())
        os.kill(pid, sig)
        return True
    except IOError, ex:
        if ex.errno != errno.ENOENT:
            raise
    except OSError, ex:
        if ex.errno != errno.ESRCH:
            raise
    return False

#
# daemon mode
#

def daemonize():
    """Turn the process into daemon.
    
    Goes background and disables all i/o.
    """

    # launch new process, kill parent
    pid = os.fork()
    if pid != 0:
        os._exit(0)

    # start new session
    os.setsid()

    # stop i/o
    fd = os.open("/dev/null", os.O_RDWR)
    os.dup2(fd, 0)
    os.dup2(fd, 1)
    os.dup2(fd, 2)
    if fd > 2:
        os.close(fd)

#
# Pidfile locking+cleanup & daemonization combined
#

def _write_pidfile(pidfile):
    pid = os.getpid()
    f = open(pidfile, 'w')
    f.write(str(pid))
    f.close()

def run_single_process(runnable, daemon, pidfile):
    """Run runnable class, possibly daemonized, locked on pidfile."""

    # check if another process is running
    if pidfile and os.path.isfile(pidfile):
        if signal_pidfile(pidfile, 0):
            print("Pidfile exists, another process running?")
            sys.exit(1)
        else:
            print("Ignoring stale pidfile")

    # daemonize if needed and write pidfile
    if daemon:
        daemonize()
    if pidfile:
        _write_pidfile(pidfile)
    
    # run and clean pidfile later
    try:
        runnable.run()
    finally:
        if pidfile:
            try:
                os.remove(pidfile)
            except: pass

#
# logging setup
#

_log_config_done = 0
_log_init_done = {}

def _init_log(job_name, service_name, cf, log_level):
    """Logging setup happens here."""
    global _log_init_done, _log_config_done

    got_skylog = 0
    use_skylog = cf.getint("use_skylog", 0)

    # load logging config if needed
    if use_skylog and not _log_config_done:
        # python logging.config braindamage:
        # cannot specify external classess without such hack
        logging.skylog = skytools.skylog
        skytools.skylog.set_service_name(service_name)

        # load general config
        flist = ['skylog.ini', '~/.skylog.ini', '/etc/skylog.ini']
        for fn in flist:
            fn = os.path.expanduser(fn)
            if os.path.isfile(fn):
                defs = {'job_name': job_name, 'service_name': service_name}
                logging.config.fileConfig(fn, defs)
                got_skylog = 1
                break
        _log_config_done = 1
        if not got_skylog:
            sys.stderr.write("skylog.ini not found!\n")
            sys.exit(1)

    # avoid duplicate logging init for job_name
    log = logging.getLogger(job_name)
    if job_name in _log_init_done:
        return log
    _log_init_done[job_name] = 1

    # compatibility: specify ini file in script config
    logfile = cf.getfile("logfile", "")
    if logfile:
        fmt = logging.Formatter('%(asctime)s %(process)s %(levelname)s %(message)s')
        size = cf.getint('log_size', 10*1024*1024)
        num = cf.getint('log_count', 3)
        hdlr = logging.handlers.RotatingFileHandler(
                    logfile, 'a', size, num)
        hdlr.setFormatter(fmt)
        log.addHandler(hdlr)

    # if skylog.ini is disabled or not available, log at least to stderr
    if not got_skylog:
        hdlr = logging.StreamHandler()
        fmt = logging.Formatter('%(asctime)s %(process)s %(levelname)s %(message)s')
        hdlr.setFormatter(fmt)
        log.addHandler(hdlr)

    log.setLevel(log_level)

    return log

class DBCachedConn(object):
    """Cache a db connection."""
    def __init__(self, name, loc, max_age = DEF_CONN_AGE, verbose = False, setup_func=None):
        self.name = name
        self.loc = loc
        self.conn = None
        self.conn_time = 0
        self.max_age = max_age
        self.autocommit = -1
        self.isolation_level = I_DEFAULT
        self.verbose = verbose
        self.setup_func = setup_func

    def get_connection(self, autocommit = 0, isolation_level = I_DEFAULT):
        # autocommit overrider isolation_level
        if autocommit:
            if isolation_level == I_SERIALIZABLE:
                raise Exception('autocommit is not compatible with I_SERIALIZABLE')
            isolation_level = I_AUTOCOMMIT

        # default isolation_level is READ COMMITTED
        if isolation_level < 0:
            isolation_level = I_READ_COMMITTED

        # new conn?
        if not self.conn:
            self.isolation_level = isolation_level
            self.conn = connect_database(self.loc)
            self.conn.my_name = self.name

            self.conn.set_isolation_level(isolation_level)
            self.conn_time = time.time()
            if self.setup_func:
                self.setup_func(self.name, self.conn)
        else:
            if self.isolation_level != isolation_level:
                raise Exception("Conflict in isolation_level")

        # done
        return self.conn

    def refresh(self):
        if not self.conn:
            return
        #for row in self.conn.notifies():
        #    if row[0].lower() == "reload":
        #        self.reset()
        #        return
        if not self.max_age:
            return
        if time.time() - self.conn_time >= self.max_age:
            self.reset()

    def reset(self):
        if not self.conn:
            return

        # drop reference
        conn = self.conn
        self.conn = None

        if self.isolation_level == I_AUTOCOMMIT:
            return

        # rollback & close
        try:
            conn.rollback()
        except: pass
        try:
            conn.close()
        except: pass

class DBScript(object):
    """Base class for database scripts.

    Handles logging, daemonizing, config, errors.
    """
    service_name = None
    job_name = None
    cf = None
    log = None
    pidfile = None
    loop_delay = 1

    def __init__(self, service_name, args):
        """Script setup.

        User class should override work() and optionally __init__(), startup(),
        reload(), reset() and init_optparse().

        NB: in case of daemon, the __init__() and startup()/work() will be
        run in different processes.  So nothing fancy should be done in __init__().
        
        @param service_name: unique name for script.
            It will be also default job_name, if not specified in config.
        @param args: cmdline args (sys.argv[1:]), but can be overrided
        """
        self.service_name = service_name
        self.db_cache = {}
        self.go_daemon = 0
        self.do_single_loop = 0
        self.looping = 1
        self.need_reload = 1
        self.stat_dict = {}
        self.log_level = logging.INFO
        self.work_state = 1

        # parse command line
        parser = self.init_optparse()
        self.options, self.args = parser.parse_args(args)

        # check args
        if self.options.daemon:
            self.go_daemon = 1
        if self.options.quiet:
            self.log_level = logging.WARNING
        if self.options.verbose:
            self.log_level = logging.DEBUG
        if len(self.args) < 1:
            print("need config file, use --help for help.")
            sys.exit(1)

        # read config file
        self.cf = self.load_config()
        self.reload()

        # init logging
        self.log = _init_log(self.job_name, self.service_name, self.cf, self.log_level)

        # send signal, if needed
        if self.options.cmd == "kill":
            self.send_signal(signal.SIGTERM)
        elif self.options.cmd == "stop":
            self.send_signal(signal.SIGINT)
        elif self.options.cmd == "reload":
            self.send_signal(signal.SIGHUP)

    def load_config(self):
        """Loads and returns skytools.Config instance.

        By default it uses first command-line argument as config
        file name.  Can be overrided.
        """

        conf_file = self.args[0]
        return Config(self.service_name, conf_file)

    def init_optparse(self, parser = None):
        """Initialize a OptionParser() instance that will be used to 
        parse command line arguments.

        Note that it can be overrided both directions - either DBScript
        will initialize a instance and passes to user code or user can
        initialize and then pass to DBScript.init_optparse().

        @param parser: optional OptionParser() instance,
               where DBScript should attachs its own arguments.
        @return: initialized OptionParser() instance.
        """
        if parser:
            p = parser
        else:
            p = optparse.OptionParser()
            p.set_usage("%prog [options] INI")
        # generic options
        p.add_option("-q", "--quiet", action="store_true",
                     help = "make program silent")
        p.add_option("-v", "--verbose", action="store_true",
                     help = "make program verbose")
        p.add_option("-d", "--daemon", action="store_true",
                     help = "go background")

        # control options
        g = optparse.OptionGroup(p, 'control running process')
        g.add_option("-r", "--reload",
                     action="store_const", const="reload", dest="cmd",
                     help = "reload config (send SIGHUP)")
        g.add_option("-s", "--stop",
                     action="store_const", const="stop", dest="cmd",
                     help = "stop program safely (send SIGINT)")
        g.add_option("-k", "--kill",
                     action="store_const", const="kill", dest="cmd",
                     help = "kill program immidiately (send SIGTERM)")
        p.add_option_group(g)

        return p

    def send_signal(self, sig):
        if not self.pidfile:
            self.log.warning("No pidfile in config, nothing todo")
        elif os.path.isfile(self.pidfile):
            alive = signal_pidfile(self.pidfile, sig)
            if not alive:
                self.log.warning("pidfile exist, but process not running")
        else:
            self.log.warning("No pidfile, process not running")
        sys.exit(0)

    def set_single_loop(self, do_single_loop):
        """Changes whether the script will loop or not."""
        self.do_single_loop = do_single_loop

    def _boot_daemon(self):
        run_single_process(self, self.go_daemon, self.pidfile)

    def start(self):
        """This will launch main processing thread."""
        if self.go_daemon:
            if not self.pidfile:
                self.log.error("Daemon needs pidfile")
                sys.exit(1)
        self.run_func_safely(self._boot_daemon)

    def stop(self):
        """Safely stops processing loop."""
        self.looping = 0

    def reload(self):
        "Reload config."
        # avoid double loading on startup
        if not self.cf:
            self.cf = self.load_config()
        else:
            self.cf.reload()
        self.job_name = self.cf.get("job_name")
        self.pidfile = self.cf.getfile("pidfile", '')
        self.loop_delay = self.cf.getfloat("loop_delay", 1.0)

    def hook_sighup(self, sig, frame):
        "Internal SIGHUP handler.  Minimal code here."
        self.need_reload = 1

    last_sigint = 0
    def hook_sigint(self, sig, frame):
        "Internal SIGINT handler.  Minimal code here."
        self.stop()
        t = time.time()
        if t - self.last_sigint < 1:
            self.log.warning("Double ^C, fast exit")
            sys.exit(1)
        self.last_sigint = t

    def stat_add(self, key, value):
        """Old, deprecated function."""
        self.stat_put(key, value)

    def stat_put(self, key, value):
        """Sets a stat value."""
        self.stat_dict[key] = value

    def stat_increase(self, key, increase = 1):
        """Increases a stat value."""
        if key in self.stat_dict:
            self.stat_dict[key] += increase
        else:
            self.stat_dict[key] = increase

    def send_stats(self):
        "Send statistics to log."

        res = []
        for k, v in self.stat_dict.items():
            res.append("%s: %s" % (k, str(v)))

        if len(res) == 0:
            return

        logmsg = "{%s}" % ", ".join(res)
        self.log.info(logmsg)
        self.stat_dict = {}

    def connection_setup(self, dbname, conn):
        pass

    def get_database(self, dbname, autocommit = 0, isolation_level = -1,
                     cache = None, connstr = None):
        """Load cached database connection.
        
        User must not store it permanently somewhere,
        as all connections will be invalidated on reset.
        """

        max_age = self.cf.getint('connection_lifetime', DEF_CONN_AGE)
        if not cache:
            cache = dbname
        if cache in self.db_cache:
            dbc = self.db_cache[cache]
        else:
            if not connstr:
                connstr = self.cf.get(dbname)
            dbc = DBCachedConn(cache, connstr, max_age, setup_func = self.connection_setup)
            self.db_cache[cache] = dbc

        return dbc.get_connection(autocommit, isolation_level)

    def close_database(self, dbname):
        """Explicitly close a cached connection.
        
        Next call to get_database() will reconnect.
        """
        if dbname in self.db_cache:
            dbc = self.db_cache[dbname]
            dbc.reset()
            del self.db_cache[dbname]

    def reset(self):
        "Something bad happened, reset all connections."
        for dbc in self.db_cache.values():
            dbc.reset()
        self.db_cache = {}

    def run(self):
        "Thread main loop."

        # run startup, safely
        self.run_func_safely(self.startup)

        while self.looping:
            # reload config, if needed
            if self.need_reload:
                self.reload()
                self.need_reload = 0

            # do some work
            work = self.run_once()

            # send stats that was added
            self.send_stats()

            # reconnect if needed
            for dbc in self.db_cache.values():
                dbc.refresh()

            # exit if needed
            if self.do_single_loop:
                self.log.debug("Only single loop requested, exiting")
                break

            # remember work state
            self.work_state = work
            # should sleep?
            if not work:
                time.sleep(self.loop_delay)

    def run_once(self):
        return self.run_func_safely(self.work, True)

    def run_func_safely(self, func, prefer_looping = False):
        "Run users work function, safely."
        try:
            return func()
        except UsageError, ex:
            self.log.error(str(ex))
            sys.exit(1)
        except SystemExit, d:
            self.send_stats()
            if prefer_looping and not self.do_single_loop:
                self.log.info("got SystemExit(%s), exiting" % str(d))
            self.reset()
            raise d
        except KeyboardInterrupt, d:
            self.send_stats()
            if prefer_looping and not self.do_single_loop:
                self.log.info("got KeyboardInterrupt, exiting")
            self.reset()
            sys.exit(1)
        except psycopg2.Error, d:
            self.send_stats()
            if d.cursor and d.cursor.connection:
                cname = d.cursor.connection.my_name
                dsn = d.cursor.connection.dsn
                sql = d.cursor.query
                self.log.error("Job %s got error on connection '%s': %s" % (
                    self.job_name,
                    d.cursor.connection.my_name,
                    str(d).strip()))
            else:
                n = "psycopg2.%s" % d.__class__.__name__
                self.log.exception("Job %s crashed: %s: %s" % (
                       self.job_name, n, str(d).rstrip()))
        except Exception, d:
            self.send_stats()
            self.log.exception("Job %s crashed: %s" % (
                       self.job_name, str(d).rstrip()))

        # reset and sleep
        self.reset()
        if prefer_looping and self.looping and not self.do_single_loop:
            time.sleep(20)
            return 1
        sys.exit(1)

    def work(self):
        """Here should user's processing happen.

        Return value is taken as boolean - if true, the next loop
        starts immidiately.  If false, DBScript sleeps for a loop_delay.
        """
        raise Exception("Nothing implemented?")

    def startup(self):
        """Will be called just before entering main loop.

        In case of daemon, if will be called in same process as work(),
        unlike __init__().
        """

        # set signals
        signal.signal(signal.SIGHUP, self.hook_sighup)
        signal.signal(signal.SIGINT, self.hook_sigint)

    def _exec_cmd(self, curs, sql, args, quiet = False):
        """Internal tool: Run SQL on cursor."""
        self.log.debug("exec_cmd: %s" % quote_statement(sql, args))
        curs.execute(sql, args)
        ok = True
        rows = curs.fetchall()
        for row in rows:
            try:
                code = row['ret_code']
                msg = row['ret_note']
            except KeyError:
                self.log.error("Query does not conform to exec_cmd API:")
                self.log.error("SQL: %s" % quote_statement(sql, args))
                self.log.error("Row: %s" % repr(row.copy()))
                sys.exit(1)
            level = code / 100
            if level == 1:
                self.log.debug("%d %s" % (code, msg))
            elif level == 2:
                if quiet:
                    self.log.debug("%d %s" % (code, msg))
                else:
                    self.log.info("%s" % (msg,))
            elif level == 3:
                self.log.warning("%s" % (msg,))
            else:
                self.log.error("%s" % (msg,))
                self.log.error("Query was: %s" % quote_statement(sql, args))
                ok = False
        return (ok, rows)

    def _exec_cmd_many(self, curs, sql, baseargs, extra_list, quiet = False):
        """Internal tool: Run SQL on cursor multiple times."""
        ok = True
        rows = []
        for a in extra_list:
            (tmp_ok, tmp_rows) = self._exec_cmd(curs, sql, baseargs + [a], quiet=quiet)
            if not tmp_ok:
                ok = False
            rows += tmp_rows
        return (ok, rows)

    def exec_cmd(self, db_or_curs, q, args, commit = True, quiet = False):
        """Run SQL on db with code/value error handling."""
        if hasattr(db_or_curs, 'cursor'):
            db = db_or_curs
            curs = db.cursor()
        else:
            db = None
            curs = db_or_curs
        (ok, rows) = self._exec_cmd(curs, q, args, quiet = quiet)
        if ok:
            if commit and db:
                db.commit()
            return rows
        else:
            if db:
                db.rollback()
            raise Exception("db error")

    def exec_cmd_many(self, db_or_curs, sql, baseargs, extra_list, commit = True, quiet = False):
        """Run SQL on db multiple times."""
        if hasattr(db_or_curs, 'cursor'):
            db = db_or_curs
            curs = db.cursor()
        else:
            db = None
            curs = db_or_curs
        (ok, rows) = self._exec_cmd_many(curs, sql, baseargs, extra_list, quiet=quiet)
        if ok:
            if commit and db:
                db.commit()
            return rows
        else:
            if db:
                db.rollback()
            raise Exception("db error")


