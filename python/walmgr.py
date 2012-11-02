#! /usr/bin/env python

"""WALShipping manager.

walmgr INI COMMAND [-n]

Master commands:
  setup              Configure PostgreSQL for WAL archiving
  sync               Copies in-progress WALs to slave
  syncdaemon         Daemon mode for regular syncing
  stop               Stop archiving - de-configure PostgreSQL
  periodic           Run periodic command if configured.
  synch-standby      Manage synchronous streaming replication.

Slave commands:
  boot               Stop playback, accept queries
  pause              Just wait, don't play WAL-s
  continue           Start playing WAL-s again

Common commands:
  init               Create configuration files, set up ssh keys.
  listbackups        List backups.
  backup             Copies all master data to slave. Will keep backup history
                     if slave keep_backups is set. EXPERIMENTAL: If run on slave,
                     creates backup from in-recovery slave data.
  restore [set][dst] Stop postmaster, move new data dir to right location and start 
                     postmaster in playback mode. Optionally use [set] as the backupset
                     name to restore. In this case the directory is copied, not moved.
  cleanup            Cleanup any walmgr files after stop.

Internal commands:
  xarchive           archive one WAL file (master)
  xrestore           restore one WAL file (slave)
  xlock              Obtain backup lock (master)
  xrelease           Release backup lock (master)
  xrotate            Rotate backup sets, expire and archive oldest if necessary.
  xpurgewals         Remove WAL files not needed for backup (slave)
  xpartialsync       Append data to WAL file (slave)
"""

import os, sys, re, signal, time, traceback
import errno, glob, ConfigParser, shutil, subprocess

import pkgloader
pkgloader.require('skytools', '3.0')

import skytools

DEFAULT_PG_VERSION = "8.3"

XLOG_SEGMENT_SIZE = 16 * 1024**2

def usage(err):
    if err > 0:
        print >>sys.stderr, __doc__
    else:
        print __doc__
    sys.exit(err)

def die(err,msg):
    print >> sys.stderr, msg
    sys.exit(err)

def yesno(prompt):
    """Ask a Yes/No question"""
    while True: 
        sys.stderr.write(prompt + " ")
        sys.stderr.flush()
        answer = sys.stdin.readline()
        if not answer:
            return False
        answer = answer.strip().lower()
        if answer in ('yes','y'):
            return True
        if answer in ('no','n'):
            return False
        sys.stderr.write("Please answer yes or no.\n")

def copy_conf(src, dst):
    """Copy config file or symlink.
    Does _not_ overwrite target.
    """
    if os.path.isdir(dst):
        dst = os.path.join(dst, os.path.basename(src))
    if os.path.exists(dst):
        return False
    if os.path.islink(src):
        linkdst = os.readlink(src)
        os.symlink(linkdst, dst)
    elif os.path.isfile(src):
        shutil.copy2(src, dst)
    else:
        raise Exception("Unsupported file type: %s" % src)
    return True

class WalChunk:
    """Represents a chunk of WAL used in record based shipping"""
    def __init__(self,filename,pos=0,bytes=0):
        self.filename = filename
        self.pos = pos
        self.bytes = bytes
        self.start_time = time.time()
        self.sync_count = 0
        self.sync_time = 0.0

    def __str__(self):
        return "%s @ %d +%d" % (self.filename, self.pos, self.bytes)

class PgControlData:
    """Contents of pg_controldata"""

    def __init__(self, bin_dir, data_dir, findRestartPoint):
        """Collect last checkpoint information from pg_controldata output"""
        self.xlogid = None
        self.xrecoff = None
        self.timeline = None
        self.wal_size = None
        self.wal_name = None
        self.cluster_state = None
        self.is_shutdown = False
        self.pg_version = 0
        self.is_valid = False

        try:
            pg_controldata = os.path.join(bin_dir, "pg_controldata")
            pipe = subprocess.Popen([ pg_controldata, data_dir ], stdout=subprocess.PIPE)
        except OSError:
            # don't complain if we cannot execute it
            return

        matches = 0
        for line in pipe.stdout.readlines():
            if findRestartPoint:
                m = re.match("^Latest checkpoint's REDO location:\s+([0-9A-F]+)/([0-9A-F]+)", line)
            else:
                m = re.match("^Latest checkpoint location:\s+([0-9A-F]+)/([0-9A-F]+)", line)
            if m:
                matches += 1
                self.xlogid = int(m.group(1), 16)
                self.xrecoff = int(m.group(2), 16)
            m = re.match("^Latest checkpoint's TimeLineID:\s+(\d+)", line)
            if m:
                matches += 1
                self.timeline = int(m.group(1))
            m = re.match("^Bytes per WAL segment:\s+(\d+)", line)
            if m:
                matches += 1
                self.wal_size = int(m.group(1))
            m = re.match("^pg_control version number:\s+(\d+)", line)
            if m:
                matches += 1
                self.pg_version = int(m.group(1))
            m = re.match("^Database cluster state:\s+(.*$)", line)
            if m:
                matches += 1
                self.cluster_state = m.group(1)
                self.is_shutdown = (self.cluster_state == "shut down")

        # ran successfully and we got our needed matches
        if pipe.wait() == 0 and matches == 5:
            self.wal_name = "%08X%08X%08X" % \
                (self.timeline, self.xlogid, self.xrecoff / self.wal_size)
            self.is_valid = True

class BackupLabel:
    """Backup label contents"""

    def __init__(self, backupdir):
        """Initialize a new BackupLabel from existing file"""
        filename = os.path.join(backupdir, "backup_label")
        self.first_wal = None
        self.start_time = None
        self.label_string = None
        if not os.path.exists(filename):
            return
        for line in open(filename):
            m = re.match('^START WAL LOCATION: [^\s]+ \(file ([0-9A-Z]+)\)$', line)
            if m:
                self.first_wal = m.group(1)
            m = re.match('^START TIME:\s(.*)$', line)
            if m:
                self.start_time = m.group(1)
            m = re.match('^LABEL: (.*)$', line)
            if m:
                self.label_string = m.group(1)

class Pgpass:
    """Manipulate pgpass contents"""

    def __init__(self, passfile):
        """Load .pgpass contents"""
        self.passfile = os.path.expanduser(passfile)
        self.contents = []

        if os.path.isfile(self.passfile):
            self.contents = open(self.passfile).readlines()

    def split_pgpass_line(selg, pgline):
        """Parses pgpass line, returns dict"""
        try:
            (host, port, db, user, pwd) = pgline.rstrip('\n\r').split(":")
            return {'host': host, 'port': port, 'db': db, 'user': user, 'pwd': pwd}
        except ValueError:
            return None

    def ensure_user(self, host, port, user, pwd):
        """Ensure that line for streaming replication exists in .pgpass"""
        self.remove_user(host, port, user)
        self.contents.insert(0, '%s:%s:%s:%s:%s\n' % (host, port, 'replication', user, pwd))

    def remove_user(self, host, port, user):
        """Remove all matching lines from .pgpass"""

        new_contents = []
        found = False
        for l in self.contents:
            p = self.split_pgpass_line(l)
            if p and p['host'] == host and p['port'] == port and p['user'] == user and p['db'] == 'replication':
                    found = True
                    continue

            new_contents.append(l)

        self.contents = new_contents
        return found

    def write(self):
        """Write contents back to file"""
        f = open(self.passfile,'w')
        os.chmod(self.passfile, 0600)
        f.writelines(self.contents)
        f.close()

    def pgpass_fields_from_conninfo(self,conninfo):
        """Extract host,user and port from primary-conninfo"""
        m = re.match("^.*\s*host=\s*([^\s]+)\s*.*$", conninfo)
        if m:
            host = m.group(1)
        else:
            host = 'localhost'
        m =  re.match("^.*\s*user=\s*([^\s]+)\s*.*$", conninfo)
        if m:
            user = m.group(1)
        else:
            user = os.environ['USER']
        m = re.match("^.*\s*port=\s*([^\s]+)\s*.*$", conninfo)
        if m:
            port = m.group(1)
        else:
            port = '5432'

        return host,port,user


class PostgresConfiguration:
    """Postgres configuration manipulation"""

    def __init__(self, walmgr, cf_file):
        """load the configuration from master_config"""
        self.walmgr = walmgr
        self.log = walmgr.log
        self.cf_file = cf_file
        self.cf_buf = open(self.cf_file, "r").read()

    def archive_mode(self):
        """Return value for specified parameter"""
        # see if explicitly set
        m = re.search("^\s*archive_mode\s*=\s*'?([a-zA-Z01]+)'?\s*#?.*$", self.cf_buf, re.M | re.I)
        if m:
            return m.group(1)
        # also, it could be commented out as initdb leaves it
        # it'd probably be best to check from the database ...
        m = re.search("^#archive_mode\s*=.*$", self.cf_buf, re.M | re.I)
        if m:
            return "off"
        return None

    def synchronous_standby_names(self):
        """Return value for specified parameter"""
        # see if explicitly set
        m = re.search("^\s*synchronous_standby_names\s*=\s*'([^']*)'\s*#?.*$", self.cf_buf, re.M | re.I)
        if m:
            return m.group(1)
        # also, it could be commented out as initdb leaves it
        # it'd probably be best to check from the database ...
        m = re.search("^#synchronous_standby_names\s*=.*$", self.cf_buf, re.M | re.I)
        if m:
            return ''
        return None

    def wal_level(self):
        """Return value for specified parameter"""
        # see if explicitly set
        m = re.search("^\s*wal_level\s*=\s*'?([a-z_]+)'?\s*#?.*$", self.cf_buf, re.M | re.I)
        if m:
            return m.group(1)
        # also, it could be commented out as initdb leaves it
        # it'd probably be best to check from the database ...
        m = re.search("^#wal_level\s*=.*$", self.cf_buf, re.M | re.I)
        if m:
            return "minimal"
        return None

    def modify(self, cf_params):
        """Change the configuration parameters supplied in cf_params"""

        for (param, value) in cf_params.iteritems():
            r_active = re.compile("^\s*%s\s*=\s*([^\s#]*).*$" % param, re.M)
            r_disabled = re.compile("^\s*#\s*%s\s*=.*$" % param, re.M)

            cf_full = "%s = '%s'" % (param, value)

            m = r_active.search(self.cf_buf)
            if m:
                old_val = m.group(1)
                self.log.debug("found parameter %s with value '%s'" % (param, old_val))
                self.cf_buf = "%s%s%s" % (self.cf_buf[:m.start()], cf_full, self.cf_buf[m.end():])
            else:
                m = r_disabled.search(self.cf_buf)
                if m:
                    self.log.debug("found disabled parameter %s" % param)
                    self.cf_buf = "%s\n%s%s" % (self.cf_buf[:m.end()], cf_full, self.cf_buf[m.end():])
                else:
                    # not found, append to the end
                    self.log.debug("found no value")
                    self.cf_buf = "%s\n%s\n\n" % (self.cf_buf, cf_full)

    def write(self):
        """Write the configuration back to file"""
        cf_old = self.cf_file + ".old"
        cf_new = self.cf_file + ".new"

        if self.walmgr.not_really:
            cf_new = "/tmp/postgresql.conf.new"
            open(cf_new, "w").write(self.cf_buf)
            self.log.info("Showing diff")
            os.system("diff -u %s %s" % (self.cf_file, cf_new))
            self.log.info("Done diff")
            os.remove(cf_new)
            return

        # polite method does not work, as usually not enough perms for it
        open(self.cf_file, "w").write(self.cf_buf)

    def set_synchronous_standby_names(self,param_value):
        """Helper function to change synchronous_standby_names and signal postmaster"""

        self.log.info("Changing synchronous_standby_names from '%s' to '%s'" % (self.synchronous_standby_names(),param_value))
        cf_params = dict()
        cf_params['synchronous_standby_names'] = param_value
        self.modify(cf_params)
        self.write()

        data_dir=self.walmgr.cf.getfile("master_data")
        self.log.info("Sending SIGHUP to postmaster")
        self.walmgr.signal_postmaster(data_dir, signal.SIGHUP)


class WalMgr(skytools.DBScript):

    def init_optparse(self, parser=None):
        p = skytools.DBScript.init_optparse(self, parser)
        p.set_usage(__doc__.strip())
        p.add_option("-n", "--not-really", action="store_true", dest="not_really",
                     help = "Don't actually do anything.", default=False)
        p.add_option("", "--init-master", action="store_true", dest="init_master",
                     help = "Initialize master walmgr.", default=False)
        p.add_option("", "--slave", action="store", type="string", dest="slave",
                     help = "Slave host name.", default="")
        p.add_option("", "--pgdata", action="store", type="string", dest="pgdata",
                     help = "Postgres data directory.", default="")
        p.add_option("", "--config-dir", action="store", type="string", dest="config_dir",
                     help = "Configuration file location for --init-X commands.", default="~/conf")
        p.add_option("", "--ssh-keygen", action="store_true", dest="ssh_keygen",
                     help = "master: generate SSH key pair if needed", default=False)
        p.add_option("", "--ssh-add-key", action="store", dest="ssh_add_key",
                     help = "slave: add public key to authorized_hosts", default=False)
        p.add_option("", "--ssh-remove-key", action="store", dest="ssh_remove_key",
                     help = "slave: remove master key from authorized_hosts", default=False)
        p.add_option("", "--add-password", action="store", dest="add_password",
                     help = "slave: add password from file to .pgpass. Additional fields will be extracted from primary-conninfo", default=False)
        p.add_option("", "--remove-password", action="store_true", dest="remove_password",
                     help = "slave: remove previously added line from .pgpass", default=False)
        p.add_option("", "--primary-conninfo", action="store", dest="primary_conninfo", default=None,
                     help = "slave: connect string for streaming replication master")
        p.add_option("", "--init-slave", action="store_true", dest="init_slave",
                     help = "Initialize slave walmgr.", default=False)
        return p

    def load_config(self):
        """override config load to allow operation without a config file"""

        if len(self.args) < 1:
            # no config file, generate default

            # guess the job name from cmdline options
            if self.options.init_master:
                job_name = 'wal-master'
            elif self.options.init_slave:
                job_name = 'wal-slave'
            else:
                job_name = 'walmgr'

            # common config settings
            opt_dict = {
                'use_skylog':       '0',
                'job_name':         job_name,
            }

            # master configuration settings
            master_opt_dict = {
                'master_db':        'dbname=template1',
                'completed_wals':   '%%(slave)s:%%(walmgr_data)s/logs.complete',
                'partial_wals':     '%%(slave)s:%%(walmgr_data)s/logs.partial',
                'full_backup':      '%%(slave)s:%%(walmgr_data)s/data.master',
                'config_backup':    '%%(slave)s:%%(walmgr_data)s/config.backup',
                'keep_symlinks':    '1',
                'compression':      '0',
                'walmgr_data':      '~/walshipping',
                'logfile':          '~/log/%(job_name)s.log',
                'pidfile':          '~/pid/%(job_name)s.pid',
                'use_skylog':       '1',
            }

            # slave configuration settings
            slave_opt_dict = {
                'completed_wals':   '%%(walmgr_data)s/logs.complete',
                'partial_wals':     '%%(walmgr_data)s/logs.partial',
                'full_backup':      '%%(walmgr_data)s/data.master',
                'config_backup':    '%%(walmgr_data)s/config.backup',
                'walmgr_data':      '~/walshipping',
                'logfile':          '~/log/%(job_name)s.log',
                'pidfile':          '~/pid/%(job_name)s.pid',
                'use_skylog':       '1',
            }

            if self.options.init_master:
                opt_dict.update(master_opt_dict)
            elif self.options.init_slave:
                opt_dict.update(slave_opt_dict)

            self.is_master = self.options.init_master

            config = skytools.Config(self.service_name, None,
                user_defs = opt_dict, override = self.cf_override)
        else:
            # default to regular config handling
            config = skytools.DBScript.load_config(self)

            self.is_master = config.has_option('master_data')

        # create the log and pid files if needed
        for cfk in [ "logfile", "pidfile" ]:
            if config.has_option(cfk):
                dirname = os.path.dirname(config.getfile(cfk))
                if not os.path.isdir(dirname):
                    os.makedirs(dirname)

        return config

    def __init__(self, args):
        skytools.DBScript.__init__(self, 'walmgr', args)
        self.set_single_loop(1)

        self.not_really = self.options.not_really
        self.pg_backup = 0
        self.walchunk = None
        self.script = os.path.abspath(sys.argv[0])

        if len(self.args) > 1:
            # normal operations, cfgfile and command
            self.cfgfile = self.args[0]
            self.cmd = self.args[1]
            self.args = self.args[2:]
        else:
            if self.options.init_master:
                self.cmd = 'init_master'
            elif self.options.init_slave:
                self.cmd = 'init_slave'
            else:
                usage(1)

            self.cfgfile = None
            self.args = []

        if self.cmd not in ('sync', 'syncdaemon'):
            # don't let pidfile interfere with normal operations, but 
            # disallow concurrent syncing
            self.pidfile = None

        cmdtab = {
            'init_master':   self.walmgr_init_master,
            'init_slave':    self.walmgr_init_slave,
            'setup':         self.walmgr_setup,
            'stop':          self.master_stop,
            'backup':        self.run_backup,
            'listbackups':   self.list_backups,
            'restore':       self.restore_database,
            'periodic':      self.master_periodic,
            'sync':          self.master_sync,
            'syncdaemon':    self.master_syncdaemon,
            'pause':         self.slave_pause,
            'continue':      self.slave_continue,
            'boot':          self.slave_boot,
            'cleanup':       self.walmgr_cleanup,
            'synch-standby': self.master_synch_standby,
            'xlock':         self.slave_lock_backups_exit,
            'xrelease':      self.slave_resume_backups,
            'xrotate':       self.slave_rotate_backups,
            'xpurgewals':    self.slave_purge_wals,
            'xarchive':      self.master_xarchive,
            'xrestore':      self.xrestore,
            'xpartialsync':  self.slave_append_partial,
        }

        if not cmdtab.has_key(self.cmd):
            usage(1)
        self.work = cmdtab[self.cmd]

    def assert_is_master(self, master_required):
        if self.is_master != master_required:
            self.log.warning("Action not available on current node.")
            sys.exit(1)

    def pg_start_backup(self, code):
        q = "select pg_start_backup('FullBackup')"
        self.log.info("Execute SQL: %s; [%s]" % (q, self.cf.get("master_db")))
        if self.not_really:
            self.pg_backup = 1
            return
        db = self.get_database("master_db")
        db.cursor().execute(q)
        db.commit()
        self.close_database("master_db")
        self.pg_backup = 1

    def pg_stop_backup(self):
        if not self.pg_backup:
            return

        q = "select pg_stop_backup()"
        self.log.debug("Execute SQL: %s; [%s]" % (q, self.cf.get("master_db")))
        if self.not_really:
            return
        db = self.get_database("master_db")
        db.cursor().execute(q)
        db.commit()
        self.close_database("master_db")

    def signal_postmaster(self, data_dir, sgn):
        pidfile = os.path.join(data_dir, "postmaster.pid")
        if not os.path.isfile(pidfile):
            self.log.info("postmaster is not running (pidfile not present)")
            return False
        buf = open(pidfile, "r").readline()
        pid = int(buf.strip())
        self.log.debug("Signal %d to process %d" % (sgn, pid))
        if sgn == 0 or not self.not_really:
            try:
                os.kill(pid, sgn)
            except OSError, ex:
                if ex.errno == errno.ESRCH:
                    self.log.info("postmaster is not running (no process at indicated PID)")
                    return False
                else:
                    raise
        return True

    def exec_rsync(self,args,die_on_error=False):
        cmdline = [ "rsync", "-a", "--quiet" ]
        if self.cf.getint("compression", 0) > 0:
            cmdline.append("-z")
        cmdline += args

        cmd = "' '".join(cmdline)
        self.log.debug("Execute rsync cmd: '%s'" % (cmd))
        if self.not_really:
            return 0
        res = os.spawnvp(os.P_WAIT, cmdline[0], cmdline)       
        if res == 24:
            self.log.info("Some files vanished, but thats OK")
            res = 0
        elif res != 0:
            self.log.fatal("rsync exec failed, res=%d" % res)
            if die_on_error:
                sys.exit(1)
        return res

    def exec_big_rsync(self, args):
        if self.exec_rsync(args) != 0:
            self.log.fatal("Big rsync failed")
            self.pg_stop_backup()
            sys.exit(1)

    def rsync_log_directory(self, source_dir, dst_loc):
        """rsync a pg_log or pg_xlog directory - ignore most of the 
        directory contents, and pay attention to symlinks
        """
        keep_symlinks = self.cf.getint("keep_symlinks", 1)

        subdir = os.path.basename(source_dir)
        if not os.path.exists(source_dir):
            self.log.info("%s does not exist, skipping" % subdir)
            return

        cmdline = []

        # if this is a symlink, copy it's target first
        if os.path.islink(source_dir) and keep_symlinks:
            self.log.info('%s is a symlink, attempting to create link target' % subdir)

            # expand the link
            link = os.readlink(source_dir)
            if not link.startswith("/"):
                link = os.path.join(os.getcwd(), link)
            link_target = os.path.join(link, "")

            slave_host = self.cf.get("slave")
            remote_target = "%s:%s" % (slave_host, link_target)
            options = [ "--include=archive_status", "--exclude=/**" ]
            if self.exec_rsync( options + [ link_target, remote_target ]):
                # unable to create the link target, just convert the links
                # to directories in PGDATA
                self.log.warning('Unable to create symlinked %s on target, copying' % subdir)
                cmdline += [ "--copy-unsafe-links" ]

        cmdline += [ "--exclude=pg_log/*" ]
        cmdline += [ "--exclude=pg_xlog/archive_status/*" ]
        cmdline += [ "--include=pg_xlog/archive_status" ]
        cmdline += [ "--exclude=pg_xlog/*" ]

        self.exec_big_rsync(cmdline + [ source_dir, dst_loc ])

    def exec_cmd(self, cmdline, allow_error=False):
        cmd = "' '".join(cmdline)
        self.log.debug("Execute cmd: '%s'" % (cmd))
        if self.not_really:
            return

        process = subprocess.Popen(cmdline,stdout=subprocess.PIPE)
        output = process.communicate()
        res = process.returncode
        
        if res != 0 and not allow_error:
            self.log.fatal("exec failed, res=%d (%s)" % (res, repr(cmdline)))
            sys.exit(1)
        return (res,output[0])

    def exec_system(self, cmdline):
        self.log.debug("Execute cmd: '%s'" % (cmdline))
        if self.not_really:
            return 0
        return os.WEXITSTATUS(os.system(cmdline))

    def chdir(self, loc):
        self.log.debug("chdir: '%s'" % (loc))
        if self.not_really:
            return
        try:
            os.chdir(loc)
        except os.error:
            self.log.fatal("CHDir failed")
            self.pg_stop_backup()
            sys.exit(1)

    def get_last_complete(self):
        """Get the name of last xarchived segment."""

        data_dir = self.cf.getfile("master_data")
        fn = os.path.join(data_dir, ".walshipping.last")
        try:
            last = open(fn, "r").read().strip()
            return last
        except:
            self.log.info("Failed to read %s" % fn)
            return None

    def set_last_complete(self, last):
        """Set the name of last xarchived segment."""

        data_dir = self.cf.getfile("master_data")
        fn = os.path.join(data_dir, ".walshipping.last")
        fn_tmp = fn + ".new"
        try:
            f = open(fn_tmp, "w")
            f.write(last)
            f.close()
            os.rename(fn_tmp, fn)
        except:
            self.log.fatal("Cannot write to %s" % fn)


    def master_stop(self):
        """Deconfigure archiving, attempt to stop syncdaemon"""
        data_dir = self.cf.getfile("master_data")
        restart_cmd = self.cf.getfile("master_restart_cmd", "")

        self.assert_is_master(True)
        self.log.info("Disabling WAL archiving")

        self.master_configure_archiving(False, restart_cmd)

        # if we have a restart command, then use it, otherwise signal
        if restart_cmd:
            self.log.info("Restarting postmaster")
            self.exec_system(restart_cmd)
        else:
            self.log.info("Sending SIGHUP to postmaster")
            self.signal_postmaster(data_dir, signal.SIGHUP)

        # stop any running syncdaemons
        pidfile = self.cf.getfile("pidfile", "")
        if os.path.exists(pidfile):
            self.log.info('Pidfile %s exists, attempting to stop syncdaemon.' % pidfile)
            self.exec_cmd([self.script, self.cfgfile, "syncdaemon", "-s"])

        self.log.info("Done")
    
    def walmgr_cleanup(self):
        """
        Clean up any walmgr files on slave and master.
        """

        if not self.is_master:
            # remove walshipping directory
            dirname = self.cf.getfile("walmgr_data")
            self.log.info("Removing walmgr data directory: %s" % dirname)
            if not self.not_really:
                shutil.rmtree(dirname)

            # remove backup 8.3/main.X directories
            backups = glob.glob(self.cf.getfile("slave_data") + ".[0-9]")
            for dirname in backups:
                self.log.info("Removing backup main directory: %s" % dirname)
                if not self.not_really:
                    shutil.rmtree(dirname)

            ssh_dir = os.path.expanduser("~/.ssh")
            auth_file = os.path.join(ssh_dir, "authorized_keys")

            if self.options.ssh_remove_key and os.path.isfile(auth_file):
                # remove master key from ssh authorized keys, simple substring match should do
                keys = ""
                for key in open(auth_file):
                    if not self.options.ssh_remove_key in key:
                        keys += key
                    else:
                        self.log.info("Removed %s from %s" % (self.options.ssh_remove_key, auth_file))

                self.log.info("Overwriting authorized_keys file")

                if not self.not_really:
                    tmpfile = auth_file + ".walmgr.tmp"
                    f = open(tmpfile, "w")
                    f.write(keys)
                    f.close()
                    os.rename(tmpfile, auth_file)
                else:
                    self.log.debug("authorized_keys:\n%s" % keys)

            # remove password from .pgpass
            primary_conninfo = self.cf.get("primary_conninfo", "")
            if self.options.remove_password and primary_conninfo and not self.not_really:
                pg = Pgpass('~/.pgpass')
                host, port, user = pg.pgpass_fields_from_conninfo(primary_conninfo)
                if pg.remove_user(host, port, user):
                    self.log.info("Removing line from .pgpass")
                    pg.write()

        # get rid of the configuration file, both master and slave
        self.log.info("Removing config file: %s" % self.cfgfile)
        if not self.not_really:
            os.remove(self.cfgfile)

    def master_synch_standby(self):
        """Manage synchronous_standby_names parameter"""

        if len(self.args) < 1:
            die(1, "usage: synch-standby SYNCHRONOUS_STANDBY_NAMES")

        names = self.args[0]
        cf = PostgresConfiguration(self, self.cf.getfile("master_config"))

        self.assert_is_master(True)

        # list of slaves
        db = self.get_database("master_db")
        cur = db.cursor()
        cur.execute("select application_name from pg_stat_replication")
        slave_names = [slave[0] for slave in cur.fetchall()]
        self.close_database("master_db")

        if names.strip() == "":
            cf.set_synchronous_standby_names("")
            return

        if names.strip() == "*":
            if slave_names:
                cf.set_synchronous_standby_names(names)
                return
            else:
                die(1,"At least one slave must be available when enabling synchronous mode")

        # ensure that at least one slave is available from new parameter value
        slave_found = None
        for new_synch_slave in re.findall(r"[^\s,]+",names):
            if new_synch_slave not in slave_names:
                self.log.warning("No slave available with name %s" % new_synch_slave)
            else:
                slave_found = True
                break

        if not slave_found:
            die(1,"At least one slave must be available from new list when enabling synchronous mode")
        else:
            cf.set_synchronous_standby_names(names)

    def master_configure_archiving(self, enable_archiving, can_restart):
        """Turn the archiving on or off"""

        cf = PostgresConfiguration(self, self.cf.getfile("master_config"))
        curr_archive_mode = cf.archive_mode()
        curr_wal_level = cf.wal_level()
        need_restart_warning = False

        if enable_archiving:
            # enable archiving
            cf_file = os.path.abspath(self.cf.filename)

            xarchive = "%s %s %s" % (self.script, cf_file, "xarchive %p %f")
            cf_params = { "archive_command": xarchive }

            if curr_archive_mode is not None:
                # archive mode specified in config, turn it on
                self.log.debug("found 'archive_mode' in config -- enabling it")
                cf_params["archive_mode"] = "on"

                if curr_archive_mode.lower() not in ('1', 'on', 'true') and not can_restart:
                    need_restart_warning = True

            if curr_wal_level is not None and curr_wal_level != 'hot_standby':
                # wal level set in config, enable it
                wal_level = self.cf.getboolean("hot_standby", False) and "hot_standby" or "archive"

                self.log.debug("found 'wal_level' in config -- setting to '%s'" % wal_level)
                cf_params["wal_level"] = wal_level

                if curr_wal_level not in ("archive", "hot_standby") and not can_restart:
                    need_restart_warning = True

            if need_restart_warning:
                self.log.warning("database must be restarted to enable archiving")

        else:
            # disable archiving
            cf_params = dict()

            if can_restart:
                # can restart, disable archive mode and set wal_level to minimal

                cf_params['archive_command'] = ''

                if curr_archive_mode:
                    cf_params['archive_mode'] = 'off'
                if curr_wal_level:
                    cf_params['wal_level'] = 'minimal'
                    cf_params['max_wal_senders'] = '0'
            else:
                # not possible to change archive_mode or wal_level (requires restart),
                # so we just set the archive_command to /bin/true to avoid WAL pileup.
                self.log.warning("database must be restarted to disable archiving")
                self.log.info("Setting archive_command to /bin/true to avoid WAL pileup")

                cf_params['archive_command'] = '/bin/true'

                # disable synchronous standbys, note that presently we don't care
                # if there is more than one standby.
                if cf.synchronous_standby_names():
                    cf_params['synchronous_standby_names'] = ''

        self.log.debug("modifying configuration: %s" % cf_params)

        cf.modify(cf_params)
        cf.write()

    def slave_deconfigure_archiving(self, cf_file):
        """Disable archiving for the slave. This is done by setting
        archive_command to a trivial command, so that archiving can be
        re-enabled without restarting postgres. Needed when slave is
        booted with postgresql.conf from master."""

        self.log.debug("Disable archiving in %s" % cf_file)

        cf = PostgresConfiguration(self, cf_file)
        cf_params = { "archive_command": "/bin/true" }

        self.log.debug("modifying configuration: %s" % cf_params)
        cf.modify(cf_params)
        cf.write()

    def remote_mkdir(self, remdir):
        tmp = remdir.split(":", 1)
        if len(tmp) < 1:
            raise Exception("cannot find pathname")
        elif len(tmp) < 2:
            self.exec_cmd([ "mkdir", "-p", tmp[0] ])
        else:
            host, path = tmp
            cmdline = ["ssh", "-nT", host, "mkdir", "-p", path]
            self.exec_cmd(cmdline)

    def remote_walmgr(self, command, stdin_disabled = True, allow_error=False):
        """Pass a command to slave WalManager"""

        sshopt = "-T"
        if stdin_disabled:
            sshopt += "n"

        slave_config = self.cf.getfile("slave_config")
        if not slave_config:
            raise Exception("slave_config not specified in %s" % self.cfgfile)

        slave_host = self.cf.get("slave")
        cmdline = [ "ssh", sshopt, "-o", "Batchmode=yes", "-o", "StrictHostKeyChecking=no",
                    slave_host, self.script, slave_config, command ]

        if self.not_really:
            cmdline += ["--not-really"]

        return self.exec_cmd(cmdline, allow_error)

    def remote_xlock(self):
        """
        Obtain the backup lock to ensure that several backups are not
        run in parralel. If someone already has the lock we check if
        this is from a previous (failed) backup. If that is the case,
        the lock is released and re-obtained.
        """
        xlock_cmd = "xlock %d" % os.getpid()
        ret = self.remote_walmgr(xlock_cmd, allow_error=True)
        if ret[0] != 0:
            # lock failed.
            try:
                lock_pid = int(ret[1])
            except ValueError:
                self.log.fatal("Invalid pid in backup lock")
                sys.exit(1)

            try:
                os.kill(lock_pid, 0)
                self.log.fatal("Backup lock already taken")
                sys.exit(1)
            except OSError:
                # no process, carry on
                self.remote_walmgr("xrelease")
                self.remote_walmgr(xlock_cmd)

    def override_cf_option(self, option, value):
        """Set a configuration option, if it is unset"""
        if not self.cf.has_option(option):
            self.cf.cf.set('walmgr', option, value)

    def guess_locations(self):
        """
        Guess PGDATA and configuration file locations.
        """

        # find the PGDATA directory
        if self.options.pgdata:
            self.pgdata = self.options.pgdata
        elif 'PGDATA' in os.environ:
            self.pgdata = os.environ['PGDATA']
        else:
            self.pgdata = "~/%s/main" % DEFAULT_PG_VERSION

        self.pgdata = os.path.expanduser(self.pgdata)
        if not os.path.isdir(self.pgdata):
            die(1, 'Postgres data directory not found: %s' % self.pgdata)

        postmaster_opts = os.path.join(self.pgdata, 'postmaster.opts')
        self.postgres_bin = ""
        self.postgres_conf = ""

        if os.path.exists(postmaster_opts):
            # postmaster_opts exists, attempt to guess various paths

            # get unquoted args from opts file
            cmdline = [ k.strip('"') for k in open(postmaster_opts).read().split() ]

            if cmdline:
                self.postgres_bin = os.path.dirname(cmdline[0])
                cmdline = cmdline[1:]

            for item in cmdline:
                if item.startswith("config_file="):
                    self.postgres_conf = item.split("=")[1]

            if not self.postgres_conf:
                self.postgres_conf = os.path.join(self.pgdata, "postgresql.conf")

        else:
            # no postmaster opts, resort to guessing

            self.log.info('postmaster.opts not found, resorting to guesses')

            # use the directory of first postgres executable from path
            for path in os.environ['PATH'].split(os.pathsep):
                path = os.path.expanduser(path)
                exe = os.path.join(path, "postgres")
                if os.path.isfile(exe):
                    self.postgres_bin = path
                    break
            else:
                # not found, use Debian default
                self.postgres_bin = "/usr/lib/postgresql/%s/bin" % DEFAULT_PG_VERSION

            if os.path.exists(self.pgdata):
                self.postgres_conf = os.path.join(self.pgdata, "postgresql.conf")
            else:
                self.postgres_conf = "/etc/postgresql/%s/main/postgresql.conf" % DEFAULT_PG_VERSION

        if not os.path.isdir(self.postgres_bin):
            die(1, "Postgres bin directory not found.")

        if not os.path.isfile(self.postgres_conf):
            if not self.options.init_slave:
                # postgres_conf is required for master
                die(1, "Configuration file not found: %s" % self.postgres_conf)

        # Attempt to guess the init.d script name
        script_suffixes = [ "9.1", "9.0", "8.4", "8.3", "8.2", "8.1", "8.0" ]
        self.initd_script = "/etc/init.d/postgresql"
        if not os.path.exists(self.initd_script):
            for suffix in script_suffixes:
                try_file = "%s-%s" % (self.initd_script, suffix)
                if os.path.exists(try_file):
                    self.initd_script = try_file
                    break
            else:
                self.initd_script = "%s -m fast -D %s" % \
                    (os.path.join(self.postgres_bin, "pg_ctl"), os.path.abspath(self.pgdata))

    def write_walmgr_config(self, config_data):
        cf_name = os.path.join(os.path.expanduser(self.options.config_dir),
                    self.cf.get("job_name") + ".ini")

        dirname = os.path.dirname(cf_name)
        if not os.path.isdir(dirname):
            self.log.info('Creating config directory: %s' % dirname)
            os.makedirs(dirname)

        self.log.info('Writing configuration file: %s' % cf_name)
        self.log.debug("config data:\n%s" % config_data)
        if not self.not_really:
            cf = open(cf_name, "w")
            cf.write(config_data)
            cf.close()

    def walmgr_init_master(self):
        """
        Initialize configuration file, generate SSH key pair if needed.
        """

        self.guess_locations()

        if not self.options.slave:
            die(1, 'Specify slave host name with "--slave" option.')

        self.override_cf_option('master_bin', self.postgres_bin)
        self.override_cf_option('master_config', self.postgres_conf)
        self.override_cf_option('master_data', self.pgdata)

        # assume that slave config is in the same location as master's
        # can override with --set slave_config=
        slave_walmgr_dir = os.path.abspath(os.path.expanduser(self.options.config_dir))
        self.override_cf_option('slave_config', os.path.join(slave_walmgr_dir, "wal-slave.ini"))

        master_config = """[walmgr]
job_name            = %(job_name)s
logfile             = %(logfile)s
pidfile             = %(pidfile)s
use_skylog          = 1

master_db           = %(master_db)s
master_data         = %(master_data)s
master_config       = %(master_config)s
master_bin          = %(master_bin)s

slave               = %(slave)s
slave_config        = %(slave_config)s

walmgr_data         = %(walmgr_data)s
completed_wals      = %(completed_wals)s
partial_wals        = %(partial_wals)s
full_backup         = %(full_backup)s
config_backup       = %(config_backup)s

keep_symlinks       = %(keep_symlinks)s
compression         = %(compression)s
"""

        try:
            opt_dict = dict([(k, self.cf.get(k)) for k in self.cf.options()])
            opt_dict['slave'] = self.options.slave
            master_config = master_config % opt_dict
        except KeyError, e:
            die(1, 'Required setting missing: %s' % e)

        self.write_walmgr_config(master_config)

        # generate SSH key pair if requested
        if self.options.ssh_keygen:
            keyfile = os.path.expanduser("~/.ssh/id_dsa")
            if os.path.isfile(keyfile):
                self.log.info("SSH key %s already exists, skipping" % keyfile)
            else:
                self.log.info("Generating ssh key: %s" % keyfile)
                cmdline = ["ssh-keygen", "-t", "dsa", "-N", "", "-q", "-f", keyfile ]
                self.log.debug(' '.join(cmdline))
                if not self.not_really:
                    subprocess.call(cmdline)
                key = open(keyfile + ".pub").read().strip()
                self.log.info("public key: %s" % key)

    def walmgr_init_slave(self):
        """
        Initialize configuration file, move SSH pubkey into place.
        """
        self.guess_locations()

        self.override_cf_option('slave_bin', self.postgres_bin)
        self.override_cf_option('slave_data', self.pgdata)
        self.override_cf_option('slave_config_dir', os.path.dirname(self.postgres_conf))

        if self.initd_script:
            self.override_cf_option('slave_start_cmd', "%s start" % self.initd_script)
            self.override_cf_option('slave_stop_cmd', "%s stop" % self.initd_script)

        slave_config = """[walmgr]
job_name             = %(job_name)s
logfile              = %(logfile)s
use_skylog           = %(use_skylog)s

slave_data           = %(slave_data)s
slave_bin            = %(slave_bin)s
slave_stop_cmd       = %(slave_stop_cmd)s
slave_start_cmd      = %(slave_start_cmd)s
slave_config_dir     = %(slave_config_dir)s

walmgr_data          = %(walmgr_data)s
completed_wals       = %(completed_wals)s
partial_wals         = %(partial_wals)s
full_backup          = %(full_backup)s
config_backup        = %(config_backup)s
"""

        if self.options.primary_conninfo:
            self.override_cf_option('primary_conninfo', self.options.primary_conninfo)
            slave_config += """
primary_conninfo     = %(primary_conninfo)s
"""

        try:
            opt_dict = dict([(k, self.cf.get(k)) for k in self.cf.options()])
            slave_config = slave_config % opt_dict
        except KeyError, e:
            die(1, 'Required setting missing: %s' % e)

        self.write_walmgr_config(slave_config)

        if self.options.ssh_add_key:
            # add the named public key to authorized hosts
            ssh_dir = os.path.expanduser("~/.ssh")
            auth_file = os.path.join(ssh_dir, "authorized_keys")

            if not os.path.isdir(ssh_dir):
                self.log.info("Creating directory: %s" % ssh_dir)
                if not self.not_really:
                    os.mkdir(ssh_dir)

            self.log.debug("Reading public key from %s" % self.options.ssh_add_key)
            master_pubkey = open(self.options.ssh_add_key).read()

            key_present = False
            if os.path.isfile(auth_file):
                for key in open(auth_file):
                    if key == master_pubkey:
                        self.log.info("Key already present in %s, skipping" % auth_file)
                        key_present = True

            if not key_present:
                self.log.info("Adding %s to %s" % (self.options.ssh_add_key, auth_file))
                if not self.not_really:
                    af = open(auth_file, "a")
                    af.write(master_pubkey)
                    af.close()

        if self.options.add_password and self.options.primary_conninfo:
            # add password to pgpass

            self.log.debug("Reading password from file %s" % self.options.add_password)
            pwd = open(self.options.add_password).readline().rstrip('\n\r')

            pg = Pgpass('~/.pgpass')
            host, port, user = pg.pgpass_fields_from_conninfo(self.options.primary_conninfo)
            pg.ensure_user(host, port, user, pwd)
            pg.write()

            self.log.info("Added password from %s to .pgpass" % self.options.add_password)



    def walmgr_setup(self):
        if self.is_master:
            self.log.info("Configuring WAL archiving")

            data_dir = self.cf.getfile("master_data")
            restart_cmd = self.cf.getfile("master_restart_cmd", "")

            self.master_configure_archiving(True, restart_cmd)

            # if we have a restart command, then use it, otherwise signal
            if restart_cmd:
                self.log.info("Restarting postmaster")
                self.exec_system(restart_cmd)
            else:
                self.log.info("Sending SIGHUP to postmaster")
                self.signal_postmaster(data_dir, signal.SIGHUP)

            # ask slave to init
            self.remote_walmgr("setup")
            self.log.info("Done")
        else:
            # create slave directory structure
            def mkdirs(dir):
                if not os.path.exists(dir):
                    self.log.debug("Creating directory %s" % dir)
                    if not self.not_really:
                        os.makedirs(dir)

            mkdirs(self.cf.getfile("completed_wals"))
            mkdirs(self.cf.getfile("partial_wals"))
            mkdirs(self.cf.getfile("full_backup"))

            cf_backup = self.cf.getfile("config_backup", "")
            if cf_backup:
                mkdirs(cf_backup)

    def master_periodic(self):
        """
        Run periodic command on master node. 

        We keep time using .walshipping.last file, so this has to be run before 
        set_last_complete()
        """

        self.assert_is_master(True)

        try:
            command_interval = self.cf.getint("command_interval", 0)
            periodic_command = self.cf.get("periodic_command", "")

            if periodic_command:
                check_file = os.path.join(self.cf.getfile("master_data"), ".walshipping.periodic")

                elapsed = 0
                if os.path.isfile(check_file):
                    elapsed = time.time() - os.stat(check_file).st_mtime

                self.log.info("Running periodic command: %s" % periodic_command)
                if not elapsed or elapsed > command_interval:
                    if not self.not_really:
                        rc = os.WEXITSTATUS(self.exec_system(periodic_command))
                        if rc != 0:
                            self.log.error("Periodic command exited with status %d" % rc)
                            # dont update timestamp - try again next time
                        else:
                            open(check_file,"w").write("1")
                else:
                    self.log.debug("%d seconds elapsed, not enough to run periodic." % elapsed)
        except Exception, det:
            self.log.error("Failed to run periodic command: %s" % str(det))

    def master_backup(self):
        """
        Copy master data directory to slave.

        1. Obtain backup lock on slave.
        2. Rotate backups on slave
        3. Perform backup as usual
        4. Purge unneeded WAL-s from slave
        5. Release backup lock
        """

        self.remote_xlock()
        errors = False

        try:
            self.pg_start_backup("FullBackup")
            self.remote_walmgr("xrotate")

            data_dir = self.cf.getfile("master_data")
            dst_loc = self.cf.getfile("full_backup")
            if dst_loc[-1] != "/":
                dst_loc += "/"

            master_spc_dir = os.path.join(data_dir, "pg_tblspc")
            slave_spc_dir = dst_loc + "tmpspc"

            # copy data
            self.chdir(data_dir)
            cmdline = [
                    "--delete",
                    "--exclude", ".*",
                    "--exclude", "*.pid",
                    "--exclude", "*.opts",
                    "--exclude", "*.conf",
                    "--exclude", "pg_xlog",
                    "--exclude", "pg_tblspc",
                    "--exclude", "pg_log",
                    "--exclude", "base/pgsql_tmp",
                    "--copy-unsafe-links",
                    ".", dst_loc]
            self.exec_big_rsync(cmdline)

            # copy tblspc first, to test
            if os.path.isdir(master_spc_dir):
                self.log.info("Checking tablespaces")
                list = os.listdir(master_spc_dir)
                if len(list) > 0:
                    self.remote_mkdir(slave_spc_dir)
                for tblspc in list:
                    if tblspc[0] == ".":
                        continue
                    tfn = os.path.join(master_spc_dir, tblspc)
                    if not os.path.islink(tfn):
                        self.log.info("Suspicious pg_tblspc entry: "+tblspc)
                        continue
                    spc_path = os.path.realpath(tfn)
                    self.log.info("Got tablespace %s: %s" % (tblspc, spc_path))
                    dstfn = slave_spc_dir + "/" + tblspc

                    try:
                        os.chdir(spc_path)
                    except Exception, det:
                        self.log.warning("Broken link:" + str(det))
                        continue
                    cmdline = [ "--delete", "--exclude", ".*", "--copy-unsafe-links", ".", dstfn]
                    self.exec_big_rsync(cmdline)

            # copy the pg_log and pg_xlog directories, these may be 
            # symlinked to nonstandard location, so pay attention
            self.rsync_log_directory(os.path.join(data_dir, "pg_log"),  dst_loc)
            self.rsync_log_directory(os.path.join(data_dir, "pg_xlog"), dst_loc)

            # copy config files
            conf_dst_loc = self.cf.getfile("config_backup", "")
            if conf_dst_loc:
                master_conf_dir = os.path.dirname(self.cf.getfile("master_config"))
                self.log.info("Backup conf files from %s" % master_conf_dir)
                self.chdir(master_conf_dir)
                cmdline = [
                     "--include", "*.conf",
                     "--exclude", "*",
                     ".", conf_dst_loc]
                self.exec_big_rsync(cmdline)

            self.remote_walmgr("xpurgewals")
        except Exception, e:
            self.log.error(e)
            errors = True
        finally:
            try:
                self.pg_stop_backup()
            except:
                pass

        try:
            self.remote_walmgr("xrelease")
        except:
            pass

        if not errors:
            self.log.info("Full backup successful")
        else:
            self.log.error("Full backup failed.")

    def slave_backup(self):
        """
        Create backup on slave host.

        1. Obtain backup lock
        2. Pause WAL apply
        3. Wait for WAL apply to complete (look at PROGRESS file)
        4. Rotate old backups
        5. Copy data directory to data.master
        6. Create backup label and history file.
        7. Purge unneeded WAL-s
        8. Resume WAL apply
        9. Release backup lock
        """
        self.assert_is_master(False)
        if self.slave_lock_backups() != 0:
            self.log.error("Cannot obtain backup lock.")
            sys.exit(1)

        try:
            self.slave_pause(waitcomplete=1)

            try:
                self.slave_rotate_backups()
                src = self.cf.getfile("slave_data")
                dst = self.cf.getfile("full_backup")

                start_time = time.localtime()
                cmdline = ["cp", "-a", src, dst ]
                self.log.info("Executing %s" % " ".join(cmdline))
                if not self.not_really:
                    self.exec_cmd(cmdline)
                stop_time = time.localtime()

                # Obtain the last restart point information
                ctl = PgControlData(self.cf.getfile("slave_bin", ""), dst, True)

                # TODO: The newly created backup directory probably still contains
                # backup_label.old and recovery.conf files. Remove these.

                if not ctl.is_valid:
                    self.log.warning("Unable to determine last restart point, backup_label not created.")
                else:
                    # Write backup label and history file
                    
                    backup_label = \
"""START WAL LOCATION: %(xlogid)X/%(xrecoff)X (file %(wal_name)s)
CHECKPOINT LOCATION: %(xlogid)X/%(xrecoff)X
START TIME: %(start_time)s
LABEL: SlaveBackup"
"""
                    backup_history = \
"""START WAL LOCATION: %(xlogid)X/%(xrecoff)X (file %(wal_name)s)
STOP WAL LOCATION: %(xlogid)X/%(xrecoff)X (file %(wal_name)s)
CHECKPOINT LOCATION: %(xlogid)X/%(xrecoff)X
START TIME: %(start_time)s
LABEL: SlaveBackup"
STOP TIME: %(stop_time)s
"""

                    label_params = {
                        "xlogid":       ctl.xlogid,
                        "xrecoff":      ctl.xrecoff,
                        "wal_name":     ctl.wal_name,
                        "start_time":   time.strftime("%Y-%m-%d %H:%M:%S %Z", start_time),
                        "stop_time":    time.strftime("%Y-%m-%d %H:%M:%S %Z", stop_time),
                    }

                    # Write the label
                    filename = os.path.join(dst, "backup_label")
                    if self.not_really:
                        self.log.info("Writing backup label to %s" % filename)
                    else:
                        lf = open(filename, "w")
                        lf.write(backup_label % label_params)
                        lf.close()

                    # Now the history
                    histfile = "%s.%08X.backup" % (ctl.wal_name, ctl.xrecoff % ctl.wal_size)
                    completed_wals = self.cf.getfile("completed_wals")
                    filename = os.path.join(completed_wals, histfile)
                    if os.path.exists(filename):
                        self.log.warning("%s: already exists, refusing to overwrite." % filename)
                    else:
                        if self.not_really:
                            self.log.info("Writing backup history to %s" % filename)
                        else:
                            lf = open(filename, "w")
                            lf.write(backup_history % label_params)
                            lf.close()

                self.slave_purge_wals()
            finally:
                self.slave_continue()
        finally:
            self.slave_resume_backups()
    
    def run_backup(self):
        if self.is_master:
            self.master_backup()
        else:
            self.slave_backup()

    def master_xarchive(self):
        """Copy a complete WAL segment to slave."""

        self.assert_is_master(True)

        if len(self.args) < 2:
            die(1, "usage: xarchive srcpath srcname")
        srcpath = self.args[0]
        srcname = self.args[1]

        start_time = time.time()
        self.log.debug("%s: start copy", srcname)
        
        self.master_periodic()
        
        dst_loc = self.cf.getfile("completed_wals")
        if dst_loc[-1] != "/":
            dst_loc += "/"

        # copy data
        self.exec_rsync([ srcpath, dst_loc ], True)

        # sync the buffers to disk - this is should reduce the chance
        # of WAL file corruption in case the slave crashes.
        slave = self.cf.get("slave")
        cmdline = ["ssh", "-nT", slave, "sync" ]
        self.exec_cmd(cmdline)

        # slave has the file now, set markers
        self.set_last_complete(srcname)

        self.log.debug("%s: done", srcname)
        end_time = time.time()
        self.stat_add('count', 1)
        self.stat_add('duration', end_time - start_time)
        self.send_stats()

    def slave_append_partial(self):
        """
        Read 'bytes' worth of data from stdin, append to the partial log file 
        starting from 'offset'. On error it is assumed that master restarts 
        from zero.
        
        The resulting file is always padded to XLOG_SEGMENT_SIZE bytes to 
        simplify recovery.
        """

        def fail(message):
            self.log.error("Slave: %s: %s" % (filename, message))
            sys.exit(1)

        self.assert_is_master(False)
        if len(self.args) < 3:
            die(1, "usage: xpartialsync <filename> <offset> <bytes>")

        filename = self.args[0]
        offset = int(self.args[1])
        bytes = int(self.args[2])

        data = sys.stdin.read(bytes)
        if len(data) != bytes:
            fail("not enough data, expected %d, got %d" % (bytes, len(data)))

        chunk = WalChunk(filename, offset, bytes)
        self.log.debug("Slave: adding to %s" % chunk)

        name = os.path.join(self.cf.getfile("partial_wals"), filename)

        if self.not_really:
            self.log.info("Adding to partial: %s" % name)
            return

        try:
            xlog = open(name, (offset == 0) and "w+" or "r+")
        except:
            fail("unable to open partial WAL: %s" % name)
        xlog.seek(offset)
        xlog.write(data)

        # padd the file to 16MB boundary, use sparse files
        padsize = XLOG_SEGMENT_SIZE - xlog.tell()
        if padsize > 0:
            xlog.seek(XLOG_SEGMENT_SIZE-1)
            xlog.write('\0')

        xlog.close()

    def master_send_partial(self, xlog_dir, chunk, daemon_mode):
        """
        Send the partial log chunk to slave. Use SSH with input redirection for the copy,
        consider other options if the overhead becomes visible.
        """

        try:
            xlog = open(os.path.join(xlog_dir, chunk.filename))
        except IOError, det:
            self.log.warning("Cannot access file %s" % chunk.filename)
            return

        xlog.seek(chunk.pos)

        # Fork the sync process
        childpid = os.fork()
        syncstart = time.time()
        if childpid == 0:
            os.dup2(xlog.fileno(), sys.stdin.fileno())
            try:
                self.remote_walmgr("xpartialsync %s %d %d" % (chunk.filename, chunk.pos, chunk.bytes), False)
            except:
                os._exit(1)
            os._exit(0)
        chunk.sync_time += (time.time() - syncstart)

        status = os.waitpid(childpid, 0)
        rc = os.WEXITSTATUS(status[1]) 
        if rc == 0:
            log = daemon_mode and self.log.debug or self.log.info
            log("sent to slave: %s" % chunk)
            chunk.pos += chunk.bytes
            chunk.sync_count += 1
        else:
            # Start from zero after an error
            chunk.pos = 0
            self.log.error("xpartialsync exited with status %d, restarting from zero." % rc)
            time.sleep(5)

    def master_syncdaemon(self):
        self.assert_is_master(True)
        self.set_single_loop(0)
        self.master_sync(True)

    def master_sync(self, daemon_mode=False):
        """
        Copy partial WAL segments to slave.

        On 8.2 set use_xlog_functions=1 in config file - this enables record based 
        walshipping. On 8.0 the only option is to sync files.

        If daemon_mode is specified it never switches from record based shipping to 
        file based shipping.
        """

        self.assert_is_master(True)

        use_xlog_functions = self.cf.getint("use_xlog_functions", False)
        data_dir = self.cf.getfile("master_data")
        xlog_dir = os.path.join(data_dir, "pg_xlog")
        master_bin = self.cf.getfile("master_bin", "")

        dst_loc = os.path.join(self.cf.getfile("partial_wals"), "")

        db = None
        if use_xlog_functions:
            try:
                db = self.get_database("master_db", autocommit=1)
            except:
                self.log.warning("Database unavailable, record based log shipping not possible.")
                if daemon_mode:
                    return

        if db:
            cur = db.cursor()
            cur.execute("select file_name, file_offset from pg_xlogfile_name_offset(pg_current_xlog_location())")
            (file_name, file_offs) = cur.fetchone()

            if not self.walchunk or self.walchunk.filename != file_name:
                # Switched to new WAL segment. Don't bother to copy the last bits - it
                # will be obsoleted by the archive_command.
                if self.walchunk and self.walchunk.sync_count > 0:
                    self.log.info("Switched in %d seconds, %f sec in %d interim syncs, avg %f"
                        % (time.time() - self.walchunk.start_time,
                        self.walchunk.sync_time,
                        self.walchunk.sync_count,
                        self.walchunk.sync_time / self.walchunk.sync_count))
                self.walchunk = WalChunk(file_name, 0, file_offs)
            else:
                self.walchunk.bytes = file_offs - self.walchunk.pos

            if self.walchunk.bytes > 0:
                self.master_send_partial(xlog_dir, self.walchunk, daemon_mode)
        else:
            files = os.listdir(xlog_dir)
            files.sort()

            last = self.get_last_complete()
            if last:
                self.log.info("%s: last complete" % last)
            else:
                self.log.info("last complete not found, copying all")

            # obtain the last checkpoint wal name, this can be used for
            # limiting the amount of WAL files to copy if the database
            # has been cleanly shut down
            ctl = PgControlData(master_bin, data_dir, False)
            checkpoint_wal = None
            if ctl.is_valid:
                if not ctl.is_shutdown:
                    # cannot rely on the checkpoint wal, should use some other method
                    self.log.info("Database state is not 'shut down', copying all")
                else:
                    # ok, the database is shut down, we can use last checkpoint wal
                    checkpoint_wal = ctl.wal_name
                    self.log.info("last checkpoint wal: %s" % checkpoint_wal)
            else:
                self.log.info("Unable to obtain control file information, copying all")

            for fn in files:
                # check if interesting file
                if len(fn) < 10:
                    continue
                if fn[0] < "0" or fn[0] > '9':
                    continue
                if fn.find(".") > 0:
                    continue
                # check if too old
                if last:
                    dot = last.find(".")
                    if dot > 0:
                        xlast = last[:dot]
                        if fn < xlast:
                            continue
                    else:
                        if fn <= last:
                            continue
                # check if too new
                if checkpoint_wal and fn > checkpoint_wal:
                    continue

                # got interesting WAL
                xlog = os.path.join(xlog_dir, fn)
                # copy data
                self.log.info('Syncing %s' % xlog)
                if self.exec_rsync([xlog, dst_loc], not daemon_mode) != 0:
                    self.log.error('Cannot sync %s' % xlog)
                    break
            else:
                self.log.info("Partial copy done")

    def xrestore(self):
        if len(self.args) < 2:
            die(1, "usage: xrestore srcname dstpath [last restartpoint wal]")
        srcname = self.args[0]
        dstpath = self.args[1]
        lstname = None
        if len(self.args) > 2:
            lstname = self.args[2]
        if self.is_master:
            self.master_xrestore(srcname, dstpath)
        else:
            self.slave_xrestore_unsafe(srcname, dstpath, os.getppid(), lstname)

    def slave_xrestore(self, srcname, dstpath):
        loop = 1
        ppid = os.getppid()
        while loop:
            try:
                self.slave_xrestore_unsafe(srcname, dstpath, ppid)
                loop = 0
            except SystemExit, d:
                sys.exit(1)
            except Exception, d:
                exc, msg, tb = sys.exc_info()
                self.log.fatal("xrestore %s crashed: %s: '%s' (%s: %s)" % (
                           srcname, str(exc), str(msg).rstrip(),
                           str(tb), repr(traceback.format_tb(tb))))
                del tb
                time.sleep(10)
                self.log.info("Re-exec: %s", repr(sys.argv))
                os.execv(sys.argv[0], sys.argv)

    def master_xrestore(self, srcname, dstpath):
        """
        Restore the xlog file from slave.
        """
        paths = [ self.cf.getfile("completed_wals"), self.cf.getfile("partial_wals") ]
        
        self.log.info("Restore %s to %s" % (srcname, dstpath))
        for src in paths:
            self.log.debug("Looking in %s" % src)
            srcfile = os.path.join(src, srcname)
            if self.exec_rsync([srcfile, dstpath]) == 0:
                return
        self.log.warning("Could not restore file %s" % srcname)

    def is_parent_alive(self, parent_pid):
        if os.getppid() != parent_pid or parent_pid <= 1:
            return False
        return True

    def slave_xrestore_unsafe(self, srcname, dstpath, parent_pid, lstname = None):
        srcdir = self.cf.getfile("completed_wals")
        partdir = self.cf.getfile("partial_wals")
        pausefile = os.path.join(srcdir, "PAUSE")
        stopfile = os.path.join(srcdir, "STOP")
        prgrfile = os.path.join(srcdir, "PROGRESS")
        srcfile = os.path.join(srcdir, srcname)
        partfile = os.path.join(partdir, srcname)

        # if we are using streaming replication, exit immediately 
        # if the srcfile is not here yet
        primary_conninfo = self.cf.get("primary_conninfo", "")
        if primary_conninfo and not os.path.isfile(srcfile):
            self.log.info("%s: not found (ignored)" % srcname)
            sys.exit(1)
  
        # assume that postgres has processed the WAL file and is 
        # asking for next - hence work not in progress anymore
        if os.path.isfile(prgrfile):
            os.remove(prgrfile)

        # loop until srcfile or stopfile appears
        while 1:
            if os.path.isfile(pausefile):
                self.log.info("pause requested, sleeping")
                time.sleep(20)
                continue

            if os.path.isfile(srcfile):
                self.log.info("%s: Found" % srcname)
                break

            # ignore .history files
            unused, ext = os.path.splitext(srcname)
            if ext == ".history":
                self.log.info("%s: not found, ignoring" % srcname)
                sys.exit(1)

            # if stopping, include also partial wals
            if os.path.isfile(stopfile):
                if os.path.isfile(partfile):
                    self.log.info("%s: found partial" % srcname)
                    srcfile = partfile
                    break
                else:
                    self.log.info("%s: not found, stopping" % srcname)
                    sys.exit(1)

            # nothing to do, just in case check if parent is alive
            if not self.is_parent_alive(parent_pid):
                self.log.warning("Parent dead, quitting")
                sys.exit(1)

            # nothing to do, sleep
            self.log.debug("%s: not found, sleeping" % srcname)
            time.sleep(1)

        # got one, copy it
        cmdline = ["cp", srcfile, dstpath]
        self.exec_cmd(cmdline)

        if self.cf.getint("keep_backups", 0) == 0:
            # cleanup only if we don't keep backup history, keep the files needed
            # to roll forward from last restart point. If the restart point is not
            # handed to us (i.e 8.3 or later), then calculate it ourselves.
            # Note that historic WAL files are removed during backup rotation
            if lstname == None:
                lstname = self.last_restart_point(srcname)
                self.log.debug("calculated restart point: %s" % lstname)
            else:
                self.log.debug("using supplied restart point: %s" % lstname)
            self.log.debug("%s: copy done, cleanup" % srcname)
            self.slave_cleanup(lstname)

        # create a PROGRESS file to notify that postgres is processing the WAL
        open(prgrfile, "w").write("1")

        # it would be nice to have apply time too
        self.stat_add('count', 1)
        self.send_stats()

    def restore_database(self):
        """Restore the database from backup

        If setname is specified, the contents of that backup set directory are 
        restored instead of "full_backup". Also copy is used instead of rename to 
        restore the directory (unless a pg_xlog directory has been specified).

        Restore to altdst if specified. Complain if it exists.
        """

        setname = len(self.args) > 0 and self.args[0] or None
        altdst  = len(self.args) > 1 and self.args[1] or None

        if not self.is_master:
            data_dir = self.cf.getfile("slave_data")
            stop_cmd = self.cf.getfile("slave_stop_cmd", "")
            start_cmd = self.cf.getfile("slave_start_cmd")
            pidfile = os.path.join(data_dir, "postmaster.pid")
        else:
            if not setname or not altdst:
                die(1, "Source and target directories must be specified if running on master node.")
            data_dir = altdst
            stop_cmd = None
            pidfile = None

        if setname:
            full_dir = os.path.join(self.cf.getfile("walmgr_data"), setname)
        else:
            full_dir = self.cf.getfile("full_backup")

        # stop postmaster if ordered
        if stop_cmd and os.path.isfile(pidfile):
            self.log.info("Stopping postmaster: " + stop_cmd)
            self.exec_system(stop_cmd)
            time.sleep(3)

        # is it dead?
        if pidfile and os.path.isfile(pidfile):
            self.log.info("Pidfile exists, checking if process is running.")
            if self.signal_postmaster(data_dir, 0):
                self.log.fatal("Postmaster still running.  Cannot continue.")
                sys.exit(1)

        # find name for data backup
        i = 0
        while 1:
            bak = "%s.%d" % (data_dir.rstrip("/"), i)
            if not os.path.isdir(bak):
                break
            i += 1

        if self.is_master:
            print >>sys.stderr, "About to restore to directory %s. The postgres cluster should be shut down." % data_dir
            if not yesno("Is postgres shut down on %s ?" % data_dir):
                die(1, "Shut it down and try again.")

        if not self.is_master:
            createbackup = True
        elif os.path.isdir(data_dir):
            createbackup = yesno("Create backup of %s?" % data_dir)
        else:
            # nothing to back up
            createbackup = False

        # see if we have to make a backup of the data directory 
        backup_datadir = self.cf.getboolean('backup_datadir', True)

        if os.path.isdir(data_dir) and not backup_datadir:
            self.log.warning('backup_datadir is disabled, deleting old data dir')
            shutil.rmtree(data_dir)

        if not setname and os.path.isdir(data_dir) and backup_datadir:
            # compatibility mode - restore without a set name and data directory exists
            self.log.warning("Data directory already exists, moving it out of the way.")
            createbackup = True

        # move old data away
        if createbackup and os.path.isdir(data_dir):
            self.log.info("Move %s to %s" % (data_dir, bak))
            if not self.not_really:
                os.rename(data_dir, bak)

        # move new data, copy if setname specified
        self.log.info("%s %s to %s" % (setname and "Copy" or "Move", full_dir, data_dir))

        if self.cf.getfile('slave_pg_xlog', ''):
            link_xlog_dir = True
            exclude_pg_xlog = '--exclude=pg_xlog'
        else:
            link_xlog_dir = False
            exclude_pg_xlog = ''

        if not self.not_really:
            if not setname and not link_xlog_dir:
                os.rename(full_dir, data_dir)
            else:
                rsync_args=["--delete", "--no-relative", "--exclude=pg_xlog/*"]
                if exclude_pg_xlog:
                    rsync_args.append(exclude_pg_xlog)
                rsync_args += [os.path.join(full_dir, ""), data_dir]

                self.exec_rsync(rsync_args, True)

                if link_xlog_dir:
                   os.symlink(self.cf.getfile('slave_pg_xlog'), "%s/pg_xlog" % data_dir)

                if (self.is_master and createbackup and os.path.isdir(bak)):
                    # restore original xlog files to data_dir/pg_xlog   
                    # symlinked directories are dereferenced
                    self.exec_cmd(["cp", "-rL", "%s/pg_xlog/" % full_dir, "%s/pg_xlog" % data_dir ])
                else:
                    # create an archive_status directory
                    xlog_dir = os.path.join(data_dir, "pg_xlog")
                    archive_path = os.path.join(xlog_dir, "archive_status")
                    if not os.path.exists(archive_path):
                        os.mkdir(archive_path, 0700)
        else:
            data_dir = full_dir

        # copy configuration files to rotated backup directory
        if createbackup and os.path.isdir(bak):
            for cf in ('postgresql.conf', 'pg_hba.conf', 'pg_ident.conf'):
                cfsrc = os.path.join(bak, cf)
                cfdst = os.path.join(data_dir, cf)
                if os.path.exists(cfdst):
                    self.log.info("Already exists: %s" % cfdst)
                elif os.path.exists(cfsrc):
                    self.log.debug("Copy %s to %s" % (cfsrc, cfdst))
                    if not self.not_really:
                        copy_conf(cfsrc, cfdst)

        # re-link tablespaces
        spc_dir = os.path.join(data_dir, "pg_tblspc")
        tmp_dir = os.path.join(data_dir, "tmpspc")
        if not os.path.isdir(spc_dir):
            # 8.3 requires its existence
            os.mkdir(spc_dir)
        if os.path.isdir(tmp_dir):
            self.log.info("Linking tablespaces to temporary location")
            
            # don't look into spc_dir, thus allowing
            # user to move them before.  re-link only those
            # that are still in tmp_dir
            list = os.listdir(tmp_dir)
            list.sort()
            
            for d in list:
                if d[0] == ".":
                    continue
                link_loc = os.path.abspath(os.path.join(spc_dir, d))
                link_dst = os.path.abspath(os.path.join(tmp_dir, d))
                self.log.info("Linking tablespace %s to %s" % (d, link_dst))
                if not self.not_really:
                    if os.path.islink(link_loc):
                        os.remove(link_loc)
                    os.symlink(link_dst, link_loc)


        # write recovery.conf
        rconf = os.path.join(data_dir, "recovery.conf")
        cf_file = os.path.abspath(self.cf.filename)

        # determine if we can use %r in restore_command
        ctl = PgControlData(self.cf.getfile("slave_bin", ""), data_dir, True)
        if ctl.pg_version > 830:
            self.log.debug('pg_version is %s, adding %%r to restore command' % ctl.pg_version)
            restore_command = 'xrestore %f "%p" %r'
        else:
            if not ctl.is_valid:
                self.log.warning('unable to run pg_controldata, assuming pre 8.3 environment')
            else:
                self.log.debug('using pg_controldata to determine restart points')
            restore_command = 'xrestore %f "%p"'

        conf = "restore_command = '%s %s %s'\n" % (self.script, cf_file, restore_command)

        # do we have streaming replication (hot standby)
        primary_conninfo = self.cf.get("primary_conninfo", "")
        if primary_conninfo:
            conf += "standby_mode = 'on'\n"
            conf += "trigger_file = '%s'\n" % os.path.join(self.cf.getfile("completed_wals"), "STOP")
            conf += "primary_conninfo = '%s'\n" % primary_conninfo
            conf += "archive_cleanup_command = '%s %s %%r'\n" % \
                (os.path.join(self.cf.getfile("slave_bin"), "pg_archivecleanup"),
                self.cf.getfile("completed_wals"))

        self.log.info("Write %s" % rconf)
        if self.not_really:
            print conf
        else:
            f = open(rconf, "w")
            f.write(conf)
            f.close()

        # remove stopfile on slave
        if not self.is_master:
            stopfile = os.path.join(self.cf.getfile("completed_wals"), "STOP")
            if os.path.isfile(stopfile):
                self.log.info("Removing stopfile: "+stopfile)
                if not self.not_really:
                    os.remove(stopfile)

            # attempt to restore configuration. Note that we cannot
            # postpone this to boot time, as the configuration is needed
            # to start postmaster.
            self.slave_restore_config()

            # run database in recovery mode
            self.log.info("Starting postmaster: " + start_cmd)
            self.exec_system(start_cmd)
        else:
            self.log.info("Data files restored, recovery.conf created.")
            self.log.info("postgresql.conf and additional WAL files may need to be restored manually.")

    def slave_restore_config(self):
        """Restore the configuration files if target directory specified."""
        self.assert_is_master(False)

        cf_source_dir = self.cf.getfile("config_backup", "")
        cf_target_dir = self.cf.getfile("slave_config_dir", "")

        if not cf_source_dir:
            self.log.info("Configuration backup location not specified.")
            return

        if not cf_target_dir:
            self.log.info("Configuration directory not specified, config files not restored.")
            return

        if not os.path.exists(cf_target_dir):
            self.log.warning("Configuration directory does not exist: %s" % cf_target_dir)
            return

        self.log.info("Restoring configuration files")
        for cf in ('postgresql.conf', 'pg_hba.conf', 'pg_ident.conf'):
            cfsrc = os.path.join(cf_source_dir, cf)
            cfdst = os.path.join(cf_target_dir, cf)

            if not os.path.isfile(cfsrc):
                self.log.warning("Missing configuration file backup: %s" % cf)
                continue

            self.log.debug("Copy %s to %s" % (cfsrc, cfdst))
            if not self.not_really:
                copy_conf(cfsrc, cfdst)
                if cf == 'postgresql.conf':
                    self.slave_deconfigure_archiving(cfdst)
          
    def slave_boot(self):
        self.assert_is_master(False)

        srcdir = self.cf.getfile("completed_wals")
        datadir = self.cf.getfile("slave_data")
        stopfile = os.path.join(srcdir, "STOP")

        if self.not_really:
            self.log.info("Writing STOP file: %s" % stopfile)
        else:
            open(stopfile, "w").write("1")
        self.log.info("Stopping recovery mode")


    def slave_pause(self, waitcomplete=0):
        """Pause the WAL apply, wait until last file applied if needed"""
        self.assert_is_master(False)
        srcdir = self.cf.getfile("completed_wals")
        pausefile = os.path.join(srcdir, "PAUSE")
        if not self.not_really:
            open(pausefile, "w").write("1")
        else:
            self.log.info("Writing PAUSE file: %s" % pausefile)
        self.log.info("Pausing recovery mode")

        # wait for log apply to complete
        if waitcomplete:
            prgrfile = os.path.join(srcdir, "PROGRESS")
            stopfile = os.path.join(srcdir, "STOP")
            if os.path.isfile(stopfile):
                self.log.warning("Recovery is stopped, backup is invalid if the database is open.")
                return
            while os.path.isfile(prgrfile):
                self.log.info("Waiting for WAL processing to complete ...")
                if self.not_really:
                    return
                time.sleep(1)

    def slave_continue(self):
        self.assert_is_master(False)
        srcdir = self.cf.getfile("completed_wals")
        pausefile = os.path.join(srcdir, "PAUSE")
        if os.path.isfile(pausefile):
            if not self.not_really:
                os.remove(pausefile)
            self.log.info("Continuing with recovery")
        else:
            self.log.info("Recovery not paused?")

    def slave_lock_backups_exit(self):
        """Exit with lock acquired status"""
        self.assert_is_master(False)
        sys.exit(self.slave_lock_backups())

    def slave_lock_backups(self):
        """Create lock file to deny other concurrent backups"""
        srcdir = self.cf.getfile("completed_wals")
        lockfile = os.path.join(srcdir, "BACKUPLOCK")
        if os.path.isfile(lockfile):
            self.log.warning("Somebody already has the backup lock.")
            lockfilehandle = open(lockfile,"r")
            pidstring = lockfilehandle.read();
            try:
                pid = int(pidstring)
                print("%d" % pid)
            except ValueError:
                self.log.error("lock file does not contain a pid:" + pidstring)
            return 1

        if not self.not_really:
            f = open(lockfile, "w")
            if len(self.args) > 0:
                f.write(self.args[0])
            f.close()
        self.log.info("Backup lock obtained.")
        return 0

    def slave_resume_backups(self):
        """Remove backup lock file, allow other backups to run"""
        self.assert_is_master(False)
        srcdir = self.cf.getfile("completed_wals")
        lockfile = os.path.join(srcdir, "BACKUPLOCK")
        if os.path.isfile(lockfile):
            if not self.not_really:
                os.remove(lockfile)
            self.log.info("Backup lock released.")
        else:
            self.log.info("Backup lock not held.")

    def list_backups(self):
        """List available backups. On master this just calls slave listbackups via SSH"""
        if self.is_master:
            self.remote_walmgr("listbackups")
        else:
            backups = self.get_backup_list(self.cf.getfile("full_backup"))
            if backups:
                print "\nList of backups:\n"
                print "%-15s %-24s %-11s %-24s" % \
                    ("Backup set", "Timestamp", "Label", "First WAL")
                print "%s %s %s %s" % (15*'-', 24*'-', 11*'-',24*'-')
                for backup in backups:
                    lbl = BackupLabel(backup)
                    print "%-15s %-24.24s %-11.11s %-24s" % \
                        (os.path.basename(backup), lbl.start_time,
                        lbl.label_string, lbl.first_wal)
                print
            else:
                print "\nNo backups found.\n"

    def get_first_walname(self,backupdir):
        """Returns the name of the first needed WAL segment for backupset"""
        label = BackupLabel(backupdir)
        if not label.first_wal:
            self.log.error("WAL name not found at %s" % backupdir)
            return None
        return label.first_wal

    def last_restart_point(self,walname):
        """
        Determine the WAL file of the last restart point (recovery checkpoint).
        For 8.3 this could be done with %r parameter to restore_command, for 8.2
        we need to consult control file (parse pg_controldata output).
        """
        slave_data = self.cf.getfile("slave_data")
        backup_label = os.path.join(slave_data, "backup_label")
        if os.path.exists(backup_label):
            # Label file still exists, use it for determining the restart point
            lbl = BackupLabel(slave_data)
            self.log.debug("Last restart point from backup_label: %s" % lbl.first_wal)
            return lbl.first_wal

        ctl = PgControlData(self.cf.getfile("slave_bin", ""), ".", True)
        if not ctl.is_valid:
            # No restart point information, use the given wal name
            self.log.warning("Unable to determine last restart point")
            return walname

        self.log.debug("Last restart point: %s" % ctl.wal_name)
        return ctl.wal_name

    def order_backupdirs(self,prefix,a,b):
        """Compare the backup directory indexes numerically"""
        prefix = os.path.abspath(prefix)

        a_indx = a[len(prefix)+1:]
        if not a_indx:
            a_indx = -1
        b_indx = b[len(prefix)+1:]
        if not b_indx:
            b_indx = -1
        return cmp(int(a_indx), int(b_indx))
        
    def get_backup_list(self,dst_loc):
        """Return the list of backup directories"""
        dirlist = glob.glob(os.path.abspath(dst_loc) + "*")
        dirlist.sort(lambda x,y: self.order_backupdirs(dst_loc, x,y))
        backupdirs = [ dir for dir in dirlist 
            if os.path.isdir(dir) and os.path.isfile(os.path.join(dir, "backup_label"))
                or os.path.isfile(os.path.join(dir, "backup_label.old"))]
        return backupdirs

    def slave_purge_wals(self):
        """
        Remove WAL files not needed for recovery
        """
        self.assert_is_master(False)
        backups = self.get_backup_list(self.cf.getfile("full_backup"))
        if backups:
            lastwal = self.get_first_walname(backups[-1])
            if lastwal:
                self.log.info("First useful WAL file is: %s" % lastwal)
                self.slave_cleanup(lastwal)
        else:
            self.log.debug("No WAL-s to clean up.")

    def slave_rotate_backups(self):
        """
        Rotate backups by increasing backup directory suffixes. Note that since
        we also have to make room for next backup, we actually have 
        keep_backups - 1 backups available after this.

        Unneeded WAL files are not removed here, handled by xpurgewals command instead.
        """
        self.assert_is_master(False)
        dst_loc = self.cf.getfile("full_backup")
        maxbackups = self.cf.getint("keep_backups", 0)
        archive_command = self.cf.get("archive_command", "")

        backupdirs = self.get_backup_list(dst_loc)
        if not backupdirs or maxbackups < 1:
            self.log.debug("Nothing to rotate")
        
        # remove expired backups
        while len(backupdirs) >= maxbackups and len(backupdirs) > 0:
            last = backupdirs.pop()

            # if archive_command is set, run it before removing the directory
            # Resume only if archive command succeeds.
            if archive_command:
                cmdline = archive_command.replace("$BACKUPDIR", last)
                self.log.info("Executing archive_command: " + cmdline)
                rc = self.exec_system(cmdline)
                if rc != 0:
                    self.log.error("Backup archiving returned %d, exiting!" % rc)
                    sys.exit(1)

            self.log.info("Removing expired backup directory: %s" % last)
            if self.not_really:
                continue
            cmdline = [ "rm", "-r", last ]
            self.exec_cmd(cmdline)

        # bump the suffixes if base directory exists
        if os.path.isdir(dst_loc):
            backupdirs.sort(lambda x,y: self.order_backupdirs(dst_loc, y,x))
            for dir in backupdirs:
                (name, index) = os.path.splitext(dir)
                if not re.match('\.[0-9]+$', index):
                    name = name + index
                    index = 0
                else:
                    index = int(index[1:])+1
                self.log.debug("Rename %s to %s.%s" % (dir, name, index))
                if self.not_really:
                    continue
                os.rename(dir, "%s.%s" % (name,index))

    def slave_cleanup(self, last_applied):
        completed_wals = self.cf.getfile("completed_wals")
        partial_wals = self.cf.getfile("partial_wals")

        self.log.debug("cleaning completed wals before %s" % last_applied)
        self.del_wals(completed_wals, last_applied)

        if os.path.isdir(partial_wals):
            self.log.debug("cleaning partial wals before %s" % last_applied)
            self.del_wals(partial_wals, last_applied)
        else:
            self.log.warning("partial_wals dir does not exist: %s" % partial_wals)

        self.log.debug("cleaning done")

    def del_wals(self, path, last):
        dot = last.find(".")
        if dot > 0:
            last = last[:dot]
        list = os.listdir(path)
        list.sort()
        cur_last = None
        n = len(list)
        for i in range(n):
            fname = list[i]
            full = os.path.join(path, fname)
            if fname[0] < "0" or fname[0] > "9":
                continue
            if not fname.startswith(last[0:8]):
                # only look at WAL segments in a same timeline
                continue

            ok_del = 0
            if fname < last:
                self.log.debug("deleting %s" % full)
                if not self.not_really:
                    try:
                        os.remove(full)
                    except:
                        # don't report the errors if the file has been already removed
                        # happens due to conflicts with pg_archivecleanup for instance.
                        pass
            cur_last = fname
        return cur_last

if __name__ == "__main__":
    script = WalMgr(sys.argv[1:])
    script.start()
