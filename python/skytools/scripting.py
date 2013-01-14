
"""Useful functions and classes for database scripts.

"""

import sys, os, signal, optparse, time, errno, select
import logging, logging.handlers, logging.config

import skytools
import skytools.skylog

try:
    import skytools.installer_config
    default_skylog = skytools.installer_config.skylog
except ImportError:
    default_skylog = 0

__pychecker__ = 'no-badexcept'

__all__ = ['BaseScript', 'UsageError', 'daemonize', 'DBScript']

class UsageError(Exception):
    """User induced error."""

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

def run_single_process(runnable, daemon, pidfile):
    """Run runnable class, possibly daemonized, locked on pidfile."""

    # check if another process is running
    if pidfile and os.path.isfile(pidfile):
        if skytools.signal_pidfile(pidfile, 0):
            print("Pidfile exists, another process running?")
            sys.exit(1)
        else:
            print("Ignoring stale pidfile")

    # daemonize if needed
    if daemon:
        daemonize()

    # clean only own pidfile
    own_pidfile = False

    try:
        if pidfile:
            data = str(os.getpid())
            skytools.write_atomic(pidfile, data)
            own_pidfile = True

        runnable.run()
    finally:
        if own_pidfile:
            try:
                os.remove(pidfile)
            except: pass

#
# logging setup
#

_log_config_done = 0
_log_init_done = {}

def _load_log_config(fn, defs):
    """Fixed fileConfig."""

    # Work around fileConfig default behaviour to disable
    # not only old handlers on load (which slightly makes sense)
    # but also old logger objects (which does not make sense).

    if sys.hexversion >= 0x2060000:
        logging.config.fileConfig(fn, defs, False)
    else:
        logging.config.fileConfig(fn, defs)
        root = logging.getLogger()
        for lg in root.manager.loggerDict.values():
            lg.disabled = 0

def _init_log(job_name, service_name, cf, log_level, is_daemon):
    """Logging setup happens here."""
    global _log_init_done, _log_config_done

    got_skylog = 0
    use_skylog = cf.getint("use_skylog", default_skylog)

    # if non-daemon, avoid skylog if script is running on console.
    # set use_skylog=2 to disable.
    if not is_daemon and use_skylog == 1:
        if os.isatty(sys.stdout.fileno()):
            use_skylog = 0

    # load logging config if needed
    if use_skylog and not _log_config_done:
        # python logging.config braindamage:
        # cannot specify external classess without such hack
        logging.skylog = skytools.skylog
        skytools.skylog.set_service_name(service_name, job_name)

        # load general config
        flist = cf.getlist('skylog_locations',
                           ['skylog.ini', '~/.skylog.ini', '/etc/skylog.ini'])
        for fn in flist:
            fn = os.path.expanduser(fn)
            if os.path.isfile(fn):
                defs = {'job_name': job_name, 'service_name': service_name}
                _load_log_config(fn, defs)
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

    # tune level on root logger
    root = logging.getLogger()
    root.setLevel(log_level)

    # compatibility: specify ini file in script config
    def_fmt = '%(asctime)s %(process)s %(levelname)s %(message)s'
    def_datefmt = '' # None
    logfile = cf.getfile("logfile", "")
    if logfile:
        fstr = cf.get('logfmt_file', def_fmt)
        fstr_date = cf.get('logdatefmt_file', def_datefmt)
        if log_level < logging.INFO:
            fstr = cf.get('logfmt_file_verbose', fstr)
            fstr_date = cf.get('logdatefmt_file_verbose', fstr_date)
        fmt = logging.Formatter(fstr, fstr_date)
        size = cf.getint('log_size', 10*1024*1024)
        num = cf.getint('log_count', 3)
        hdlr = logging.handlers.RotatingFileHandler(
                    logfile, 'a', size, num)
        hdlr.setFormatter(fmt)
        root.addHandler(hdlr)

    # if skylog.ini is disabled or not available, log at least to stderr
    if not got_skylog:
        fstr = cf.get('logfmt_console', def_fmt)
        fstr_date = cf.get('logdatefmt_console', def_datefmt)
        if log_level < logging.INFO:
            fstr = cf.get('logfmt_console_verbose', fstr)
            fstr_date = cf.get('logdatefmt_console_verbose', fstr_date)
        hdlr = logging.StreamHandler()
        fmt = logging.Formatter(fstr, fstr_date)
        hdlr.setFormatter(fmt)
        root.addHandler(hdlr)

    return log


class BaseScript(object):
    """Base class for service scripts.

    Handles logging, daemonizing, config, errors.

    Config template::

        ## Parameters for skytools.BaseScript ##

        # how many seconds to sleep between work loops
        # if missing or 0, then instead sleeping, the script will exit
        loop_delay = 1.0

        # where to log
        logfile = ~/log/%(job_name)s.log

        # where to write pidfile
        pidfile = ~/pid/%(job_name)s.pid

        # per-process name to use in logging
        #job_name = %(config_name)s

        # whether centralized logging should be used
        # search-path [ ./skylog.ini, ~/.skylog.ini, /etc/skylog.ini ]
        #   0 - disabled
        #   1 - enabled, unless non-daemon on console (os.isatty())
        #   2 - always enabled
        #use_skylog = 0

        # where to find skylog.ini
        #skylog_locations = skylog.ini, ~/.skylog.ini, /etc/skylog.ini

        # how many seconds to sleep after catching a exception
        #exception_sleep = 20
    """
    service_name = None
    job_name = None
    cf = None
    cf_defaults = {}
    pidfile = None

    # >0 - sleep time if work() requests sleep
    # 0  - exit if work requests sleep
    # <0 - run work() once [same as looping=0]
    loop_delay = 0

    # 0 - run work() once
    # 1 - run work() repeatedly
    looping = 1

    # result from last work() call:
    #  1 - there is probably more work, don't sleep
    #  0 - no work, sleep before calling again
    # -1 - exception was thrown
    work_state = 1

    # setup logger here, this allows override by subclass
    log = logging.getLogger('skytools.BaseScript')

    def __init__(self, service_name, args):
        """Script setup.

        User class should override work() and optionally __init__(), startup(),
        reload(), reset(), shutdown() and init_optparse().

        NB: In case of daemon, __init__() and startup()/work()/shutdown() will be
        run in different processes.  So nothing fancy should be done in __init__().

        @param service_name: unique name for script.
            It will be also default job_name, if not specified in config.
        @param args: cmdline args (sys.argv[1:]), but can be overridden
        """
        self.service_name = service_name
        self.go_daemon = 0
        self.need_reload = 0
        self.stat_dict = {}
        self.log_level = logging.INFO

        # parse command line
        parser = self.init_optparse()
        self.options, self.args = parser.parse_args(args)

        # check args
        if self.options.version:
            self.print_version()
            sys.exit(0)
        if self.options.daemon:
            self.go_daemon = 1
        if self.options.quiet:
            self.log_level = logging.WARNING
        if self.options.verbose > 1:
            self.log_level = skytools.skylog.TRACE
        elif self.options.verbose:
            self.log_level = logging.DEBUG

        self.cf_override = {}
        if self.options.set:
            for a in self.options.set:
                k, v = a.split('=', 1)
                self.cf_override[k.strip()] = v.strip()

        if self.options.ini:
            self.print_ini()
            sys.exit(0)

        # read config file
        self.reload()

        # init logging
        _init_log(self.job_name, self.service_name, self.cf, self.log_level, self.go_daemon)

        # send signal, if needed
        if self.options.cmd == "kill":
            self.send_signal(signal.SIGTERM)
        elif self.options.cmd == "stop":
            self.send_signal(signal.SIGINT)
        elif self.options.cmd == "reload":
            self.send_signal(signal.SIGHUP)

    def print_version(self):
        service = self.service_name
        if getattr(self, '__version__', None):
            service += ' version %s' % self.__version__
        print '%s, Skytools version %s' % (service, skytools.__version__)

    def print_ini(self):
        """Prints out ini file from doc string of the script of default for dbscript

        Used by --ini option on command line.
        """

        # current service name
        print("[%s]\n" % self.service_name)

        # walk class hierarchy
        bases = [self.__class__]
        while len(bases) > 0:
            parents = []
            for c in bases:
                for p in c.__bases__:
                    if p not in parents:
                        parents.append(p)
                doc = c.__doc__
                if doc:
                    self._print_ini_frag(doc)
            bases = parents

    def _print_ini_frag(self, doc):
        # use last '::' block as config template
        pos = doc and doc.rfind('::\n') or -1
        if pos < 0:
            return
        doc = doc[pos+2 : ].rstrip()
        doc = skytools.dedent(doc)

        # merge overrided options into output
        for ln in doc.splitlines():
            vals = ln.split('=', 1)
            if len(vals) != 2:
                print(ln)
                continue

            k = vals[0].strip()
            v = vals[1].strip()
            if k and k[0] == '#':
                print(ln)
                k = k[1:]
                if k in self.cf_override:
                    print('%s = %s' % (k, self.cf_override[k]))
            elif k in self.cf_override:
                if v:
                    print('#' + ln)
                print('%s = %s' % (k, self.cf_override[k]))
            else:
                print(ln)

        print('')

    def load_config(self):
        """Loads and returns skytools.Config instance.

        By default it uses first command-line argument as config
        file name.  Can be overridden.
        """

        if len(self.args) < 1:
            print("need config file, use --help for help.")
            sys.exit(1)
        conf_file = self.args[0]
        return skytools.Config(self.service_name, conf_file,
                               user_defs = self.cf_defaults,
                               override = self.cf_override)

    def init_optparse(self, parser = None):
        """Initialize a OptionParser() instance that will be used to
        parse command line arguments.

        Note that it can be overridden both directions - either DBScript
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
                     help = "log only errors and warnings")
        p.add_option("-v", "--verbose", action="count",
                     help = "log verbosely")
        p.add_option("-d", "--daemon", action="store_true",
                     help = "go background")
        p.add_option("-V", "--version", action="store_true",
                     help = "print version info and exit")
        p.add_option("", "--ini", action="store_true",
                    help = "display sample ini file")
        p.add_option("", "--set", action="append",
                    help = "override config setting (--set 'PARAM=VAL')")

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
                     help = "kill program immediately (send SIGTERM)")
        p.add_option_group(g)

        return p

    def send_signal(self, sig):
        if not self.pidfile:
            self.log.warning("No pidfile in config, nothing to do")
        elif os.path.isfile(self.pidfile):
            alive = skytools.signal_pidfile(self.pidfile, sig)
            if not alive:
                self.log.warning("pidfile exists, but process not running")
        else:
            self.log.warning("No pidfile, process not running")
        sys.exit(0)

    def set_single_loop(self, do_single_loop):
        """Changes whether the script will loop or not."""
        if do_single_loop:
            self.looping = 0
        else:
            self.looping = 1

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
            self.log.info ("Config reloaded")
        self.job_name = self.cf.get("job_name")
        self.pidfile = self.cf.getfile("pidfile", '')
        self.loop_delay = self.cf.getfloat("loop_delay", 1.0)
        self.exception_sleep = self.cf.getfloat("exception_sleep", 20)

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

    def stat_get(self, key):
        """Reads a stat value."""
        try:
            value = self.stat_dict[key]
        except KeyError:
            value = None
        return value

    def stat_put(self, key, value):
        """Sets a stat value."""
        self.stat_dict[key] = value

    def stat_increase(self, key, increase = 1):
        """Increases a stat value."""
        try:
            self.stat_dict[key] += increase
        except KeyError:
            self.stat_dict[key] = increase

    def send_stats(self):
        "Send statistics to log."

        res = []
        for k, v in self.stat_dict.items():
            res.append("%s: %s" % (k, v))

        if len(res) == 0:
            return

        logmsg = "{%s}" % ", ".join(res)
        self.log.info(logmsg)
        self.stat_dict = {}

    def reset(self):
        "Something bad happened, reset all state."
        pass

    def run(self):
        "Thread main loop."

        # run startup, safely
        self.run_func_safely(self.startup)

        while 1:
            # reload config, if needed
            if self.need_reload:
                self.reload()
                self.need_reload = 0

            # do some work
            work = self.run_once()

            if not self.looping or self.loop_delay < 0:
                break

            # remember work state
            self.work_state = work
            # should sleep?
            if not work:
                if self.loop_delay > 0:
                    self.sleep(self.loop_delay)
                    if not self.looping:
                        break
                else:
                    break

        # run shutdown, safely?
        self.shutdown()

    def run_once(self):
        state = self.run_func_safely(self.work, True)

        # send stats that was added
        self.send_stats()

        return state

    def run_func_safely(self, func, prefer_looping = False):
        "Run users work function, safely."
        cname = None
        emsg = None
        try:
            return func()
        except UsageError, d:
            self.log.error(str(d))
            sys.exit(1)
        except MemoryError, d:
            try: # complex logging may not succeed
                self.log.exception("Job %s out of memory, exiting" % self.job_name)
            except MemoryError:
                self.log.fatal("Out of memory")
            sys.exit(1)
        except SystemExit, d:
            self.send_stats()
            if prefer_looping and self.looping and self.loop_delay > 0:
                self.log.info("got SystemExit(%s), exiting" % str(d))
            self.reset()
            raise d
        except KeyboardInterrupt, d:
            self.send_stats()
            if prefer_looping and self.looping and self.loop_delay > 0:
                self.log.info("got KeyboardInterrupt, exiting")
            self.reset()
            sys.exit(1)
        except Exception, d:
            self.send_stats()
            emsg = str(d).rstrip()
            self.reset()
            self.exception_hook(d, emsg)
        # reset and sleep
        self.reset()
        if prefer_looping and self.looping and self.loop_delay > 0:
            self.sleep(self.exception_sleep)
            return -1
        sys.exit(1)

    def sleep(self, secs):
        """Make script sleep for some amount of time."""
        try:
            time.sleep(secs)
        except IOError, ex:
            if ex.errno != errno.EINTR:
                raise

    def exception_hook(self, det, emsg):
        """Called on after exception processing.

        Can do additional logging.

        @param det: exception details
        @param emsg: exception msg
        """
        self.log.exception("Job %s crashed: %s" % (
                   self.job_name, emsg))

    def work(self):
        """Here should user's processing happen.

        Return value is taken as boolean - if true, the next loop
        starts immediately.  If false, DBScript sleeps for a loop_delay.
        """
        raise Exception("Nothing implemented?")

    def startup(self):
        """Will be called just before entering main loop.

        In case of daemon, if will be called in same process as work(),
        unlike __init__().
        """
        self.started = time.time()

        # set signals
        if hasattr(signal, 'SIGHUP'):
            signal.signal(signal.SIGHUP, self.hook_sighup)
        if hasattr(signal, 'SIGINT'):
            signal.signal(signal.SIGINT, self.hook_sigint)

    def shutdown(self):
        """Will be called just after exiting main loop.

        In case of daemon, if will be called in same process as work(),
        unlike __init__().
        """
        pass

    # define some aliases (short-cuts / backward compatibility cruft)
    stat_add = stat_put                 # Old, deprecated function.
    stat_inc = stat_increase

##
##  DBScript
##

#: how old connections need to be closed
DEF_CONN_AGE = 20*60  # 20 min

class DBScript(BaseScript):
    """Base class for database scripts.

    Handles database connection state.

    Config template::

        ## Parameters for skytools.DBScript ##

        # default lifetime for database connections (in seconds)
        #connection_lifetime = 1200
    """

    def __init__(self, service_name, args):
        """Script setup.

        User class should override work() and optionally __init__(), startup(),
        reload(), reset() and init_optparse().

        NB: in case of daemon, the __init__() and startup()/work() will be
        run in different processes.  So nothing fancy should be done in __init__().

        @param service_name: unique name for script.
            It will be also default job_name, if not specified in config.
        @param args: cmdline args (sys.argv[1:]), but can be overridden
        """
        self.db_cache = {}
        self._db_defaults = {}
        self._listen_map = {} # dbname: channel_list
        BaseScript.__init__(self, service_name, args)

    def connection_hook(self, dbname, conn):
        pass

    def set_database_defaults(self, dbname, **kwargs):
        self._db_defaults[dbname] = kwargs

    def get_database(self, dbname, autocommit = 0, isolation_level = -1,
                     cache = None, connstr = None):
        """Load cached database connection.

        User must not store it permanently somewhere,
        as all connections will be invalidated on reset.
        """

        max_age = self.cf.getint('connection_lifetime', DEF_CONN_AGE)

        if not cache:
            cache = dbname

        params = {}
        defs = self._db_defaults.get(cache, {})
        params.update(defs)
        if isolation_level >= 0:
            params['isolation_level'] = isolation_level
        elif autocommit:
            params['isolation_level'] = 0
        elif params.get('autocommit', 0):
            params['isolation_level'] = 0
        elif not 'isolation_level' in params:
            params['isolation_level'] = skytools.I_READ_COMMITTED

        if not 'max_age' in params:
            params['max_age'] = max_age

        if cache in self.db_cache:
            if connstr is None:
                connstr = self.cf.get(dbname, '')
            dbc = self.db_cache[cache]
            if connstr:
                dbc.check_connstr(connstr)
        else:
            if not connstr:
                connstr = self.cf.get(dbname)

            # connstr might contain password, it is not a good idea to log it
            filtered_connstr = connstr
            pos = connstr.lower().find('password')
            if pos >= 0:
                filtered_connstr = connstr[:pos] + ' [...]'

            self.log.debug("Connect '%s' to '%s'" % (cache, filtered_connstr))
            dbc = DBCachedConn(cache, connstr, params['max_age'], setup_func = self.connection_hook)
            self.db_cache[cache] = dbc

        clist = []
        if cache in self._listen_map:
            clist = self._listen_map[cache]

        return dbc.get_connection(params['isolation_level'], clist)

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
        BaseScript.reset(self)

    def run_once(self):
        state = BaseScript.run_once(self)

        # reconnect if needed
        for dbc in self.db_cache.values():
            dbc.refresh()

        return state

    def exception_hook(self, d, emsg):
        """Log database and query details from exception."""
        curs = getattr(d, 'cursor', None)
        conn = getattr(curs, 'connection', None)
        cname = getattr(conn, 'my_name', None)
        if cname:
            # Properly named connection
            cname = d.cursor.connection.my_name
            sql = getattr(curs, 'query', None) or '?'
            if len(sql) > 200: # avoid logging londiste huge batched queries
                sql = sql[:60] + " ..."
            self.log.exception("Job %s got error on connection '%s': %s.   Query: %s" % (
                self.job_name, cname, emsg, sql))
        else:
            BaseScript.exception_hook(self, d, emsg)

    def sleep(self, secs):
        """Make script sleep for some amount of time."""
        fdlist = []
        for dbname in self._listen_map.keys():
            if dbname not in self.db_cache:
                continue
            fd = self.db_cache[dbname].fileno()
            if fd is None:
                continue
            fdlist.append(fd)

        if not fdlist:
            return BaseScript.sleep(self, secs)

        try:
            if hasattr(select, 'poll'):
                p = select.poll()
                for fd in fdlist:
                    p.register(fd, select.POLLIN)
                p.poll(int(secs * 1000))
            else:
                select.select(fdlist, [], [], secs)
        except select.error, d:
            self.log.info('wait canceled')

    def _exec_cmd(self, curs, sql, args, quiet = False, prefix = None):
        """Internal tool: Run SQL on cursor."""
        if self.options.verbose:
            self.log.debug("exec_cmd: %s" % skytools.quote_statement(sql, args))

        _pfx = ""
        if prefix:
            _pfx = "[%s] " % prefix
        curs.execute(sql, args)
        ok = True
        rows = curs.fetchall()
        for row in rows:
            try:
                code = row['ret_code']
                msg = row['ret_note']
            except KeyError:
                self.log.error("Query does not conform to exec_cmd API:")
                self.log.error("SQL: %s" % skytools.quote_statement(sql, args))
                self.log.error("Row: %s" % repr(row.copy()))
                sys.exit(1)
            level = code / 100
            if level == 1:
                self.log.debug("%s%d %s" % (_pfx, code, msg))
            elif level == 2:
                if quiet:
                    self.log.debug("%s%d %s" % (_pfx, code, msg))
                else:
                    self.log.info("%s%s" % (_pfx, msg,))
            elif level == 3:
                self.log.warning("%s%s" % (_pfx, msg,))
            else:
                self.log.error("%s%s" % (_pfx, msg,))
                self.log.debug("Query was: %s" % skytools.quote_statement(sql, args))
                ok = False
        return (ok, rows)

    def _exec_cmd_many(self, curs, sql, baseargs, extra_list, quiet = False, prefix=None):
        """Internal tool: Run SQL on cursor multiple times."""
        ok = True
        rows = []
        for a in extra_list:
            (tmp_ok, tmp_rows) = self._exec_cmd(curs, sql, baseargs + [a], quiet, prefix)
            if not tmp_ok:
                ok = False
            rows += tmp_rows
        return (ok, rows)

    def exec_cmd(self, db_or_curs, q, args, commit = True, quiet = False, prefix = None):
        """Run SQL on db with code/value error handling."""
        if hasattr(db_or_curs, 'cursor'):
            db = db_or_curs
            curs = db.cursor()
        else:
            db = None
            curs = db_or_curs
        (ok, rows) = self._exec_cmd(curs, q, args, quiet, prefix)
        if ok:
            if commit and db:
                db.commit()
            return rows
        else:
            if db:
                db.rollback()
            if self.options.verbose:
                raise Exception("db error")
            # error is already logged
            sys.exit(1)

    def exec_cmd_many(self, db_or_curs, sql, baseargs, extra_list,
                      commit = True, quiet = False, prefix = None):
        """Run SQL on db multiple times."""
        if hasattr(db_or_curs, 'cursor'):
            db = db_or_curs
            curs = db.cursor()
        else:
            db = None
            curs = db_or_curs
        (ok, rows) = self._exec_cmd_many(curs, sql, baseargs, extra_list, quiet, prefix)
        if ok:
            if commit and db:
                db.commit()
            return rows
        else:
            if db:
                db.rollback()
            if self.options.verbose:
                raise Exception("db error")
            # error is already logged
            sys.exit(1)

    def listen(self, dbname, channel):
        """Make connection listen for specific event channel.

        Listening will be activated on next .get_database() call.

        Basically this means that DBScript.sleep() will poll for events
        on that db connection, so when event appears, script will be
        woken up.
        """
        if dbname not in self._listen_map:
            self._listen_map[dbname] = []
        clist = self._listen_map[dbname]
        if channel not in clist:
            clist.append(channel)

    def unlisten(self, dbname, channel='*'):
        """Stop connection for listening on specific event channel.

        Listening will stop on next .get_database() call.
        """
        if dbname not in self._listen_map:
            return
        if channel == '*':
            del self._listen_map[dbname]
            return
        clist = self._listen_map[dbname]
        try:
            clist.remove(channel)
        except ValueError:
            pass

class DBCachedConn(object):
    """Cache a db connection."""
    def __init__(self, name, loc, max_age = DEF_CONN_AGE, verbose = False, setup_func=None, channels=[]):
        self.name = name
        self.loc = loc
        self.conn = None
        self.conn_time = 0
        self.max_age = max_age
        self.autocommit = -1
        self.isolation_level = -1
        self.verbose = verbose
        self.setup_func = setup_func
        self.listen_channel_list = []

    def fileno(self):
        if not self.conn:
            return None
        return self.conn.cursor().fileno()

    def get_connection(self, isolation_level = -1, listen_channel_list = []):

        # default isolation_level is READ COMMITTED
        if isolation_level < 0:
            isolation_level = skytools.I_READ_COMMITTED

        # new conn?
        if not self.conn:
            self.isolation_level = isolation_level
            self.conn = skytools.connect_database(self.loc)
            self.conn.my_name = self.name

            self.conn.set_isolation_level(isolation_level)
            self.conn_time = time.time()
            if self.setup_func:
                self.setup_func(self.name, self.conn)
        else:
            if self.isolation_level != isolation_level:
                raise Exception("Conflict in isolation_level")

        self._sync_listen(listen_channel_list)

        # done
        return self.conn

    def _sync_listen(self, new_clist):
        if not new_clist and not self.listen_channel_list:
            return
        curs = self.conn.cursor()
        for ch in self.listen_channel_list:
            if ch not in new_clist:
                curs.execute("UNLISTEN %s" % skytools.quote_ident(ch))
        for ch in new_clist:
            if ch not in self.listen_channel_list:
                curs.execute("LISTEN %s" % skytools.quote_ident(ch))
        if self.isolation_level != skytools.I_AUTOCOMMIT:
            self.conn.commit()
        self.listen_channel_list = new_clist[:]

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
        self.listen_channel_list = []

        # close
        try:
            conn.close()
        except: pass

    def check_connstr(self, connstr):
        """Drop connection if connect string has changed.
        """
        if self.loc != connstr:
            self.reset()
