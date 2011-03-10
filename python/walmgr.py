#! /usr/bin/env python

"""WALShipping manager.

walmgr INI COMMAND [-n]

Master commands:
  setup              Configure PostgreSQL for WAL archiving
  sync               Copies in-progress WALs to slave
  syncdaemon         Daemon mode for regular syncing
  stop               Stop archiving - de-configure PostgreSQL
  periodic           Run periodic command if configured.

Slave commands:
  boot               Stop playback, accept queries
  pause              Just wait, don't play WAL-s
  continue           Start playing WAL-s again

Common commands:
  listbackups        List backups.
  backup             Copies all master data to slave. Will keep backup history
                     if slave keep_backups is set. EXPERIMENTAL: If run on slave,
                     creates backup from in-recovery slave data.
  restore [set][dst] Stop postmaster, move new data dir to right location and start 
                     postmaster in playback mode. Optionally use [set] as the backupset
                     name to restore. In this case the directory is copied, not moved.

Internal commands:
  xarchive           archive one WAL file (master)
  xrestore           restore one WAL file (slave)
  xlock              Obtain backup lock (master)
  xrelease           Release backup lock (master)
  xrotate            Rotate backup sets, expire and archive oldest if necessary.
  xpurgewals         Remove WAL files not needed for backup (slave)

Switches:
  -n                 no action, just print commands
"""

import os, sys, re, signal, time, traceback
import errno, glob, ConfigParser, shutil, subprocess

import skytools

MASTER = 1
SLAVE = 0

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

class WalMgr(skytools.DBScript):

    def init_optparse(self, parser=None):
        p = skytools.DBScript.init_optparse(self, parser)
        p.set_usage(__doc__.strip())
        p.add_option("-n", "--not-really", action="store_true", dest="not_really",
                     help = "Don't actually do anything.", default=False)
        return p

    def __init__(self, args):

        if len(args) == 1 and args[0] == '--version':
           skytools.DBScript.__init__(self, 'wal-master', args)

        if len(args) < 2:
            # need at least config file and command
            usage(1)

        # determine the role of the node from provided configuration
        cf = ConfigParser.ConfigParser()
        cf.read(args[0])
        for (self.wtype, self.service_name) in [ (MASTER, "wal-master"), (SLAVE, "wal-slave") ]:
            if cf.has_section(self.service_name):
                break
        else:
            print >> sys.stderr, "Invalid config file: %s" % args[0]
            sys.exit(1)

        skytools.DBScript.__init__(self, self.service_name, args)
        self.set_single_loop(1)

        self.not_really = self.options.not_really
        self.pg_backup = 0
        self.walchunk = None

        if len(self.args) < 2:
            usage(1)
        self.cfgfile = self.args[0]
        self.cmd = self.args[1]
        self.args = self.args[2:]
        self.script = os.path.abspath(sys.argv[0])

        cmdtab = {
            'setup':        self.walmgr_setup,
            'stop':         self.master_stop,
            'backup':       self.run_backup,
            'listbackups':  self.list_backups,
            'restore':      self.restore_database,
            'periodic':     self.master_periodic,
            'sync':         self.master_sync,
            'syncdaemon':   self.master_syncdaemon,
            'pause':        self.slave_pause,
            'continue':     self.slave_continue,
            'boot':         self.slave_boot,
            'xlock':        self.slave_lock_backups_exit,
            'xrelease':     self.slave_resume_backups,
            'xrotate':      self.slave_rotate_backups,
            'xpurgewals':   self.slave_purge_wals,
            'xarchive':     self.master_xarchive,
            'xrestore':     self.xrestore,
            'xpartialsync': self.slave_append_partial,
        }

        if self.cmd not in ('sync', 'syncdaemon'):
            # don't let pidfile interfere with normal operations, but 
            # disallow concurrent syncing
            self.pidfile = None

        if not cmdtab.has_key(self.cmd):
            usage(1)
        self.work = cmdtab[self.cmd]

    def assert_valid_role(self,role):
        if self.wtype != role:
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

            remote_target = "%s:%s" % (self.slave_host(), link_target)
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


    def exec_cmd(self, cmdline,allow_error=False):
        cmd = "' '".join(cmdline)
        self.log.debug("Execute cmd: '%s'" % (cmd))
        if self.not_really:
            return
        #res = os.spawnvp(os.P_WAIT, cmdline[0], cmdline)
        process = subprocess.Popen(cmdline,stdout=subprocess.PIPE)
        output=process.communicate()
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

        data_dir = self.cf.get("master_data")
        fn = os.path.join(data_dir, ".walshipping.last")
        try:
            last = open(fn, "r").read().strip()
            return last
        except:
            self.log.info("Failed to read %s" % fn)
            return None

    def set_last_complete(self, last):
        """Set the name of last xarchived segment."""

        data_dir = self.cf.get("master_data")
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
        data_dir = self.cf.get("master_data")
        restart_cmd = self.cf.get("master_restart_cmd", "")

        self.assert_valid_role(MASTER)
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
        pidfile = self.cf.get("pidfile", "")
        if os.path.exists(pidfile):
            self.log.info('Pidfile %s exists, attempting to stop syncdaemon.' % pidfile)
            self.exec_cmd([self.script, self.cfgfile, "syncdaemon", "-s"])
        self.log.info("Done")

    def master_configure_archiving(self, enable_archiving, can_restart):
        """Turn the archiving on or off"""

        cf = PostgresConfiguration(self, self.cf.get("master_config"))
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

    def slave_host(self):
        """Extract the slave hostname"""
        try:
            slave = self.cf.get("slave")
            host, path = slave.split(":", 1)
        except:
            raise Exception("invalid value for 'slave' in %s" % self.cfgfile)
        return host

    def remote_walmgr(self, command, stdin_disabled = True,allow_error=False):
        """Pass a command to slave WalManager"""

        sshopt = "-T"
        if stdin_disabled:
            sshopt += "n"

        slave_config = self.cf.get("slave_config")
        if not slave_config:
            raise Exception("slave_config not specified in %s" % self.cfgfile)

        try:
            slave = self.cf.get("slave")
            host, path = slave.split(":", 1)
        except:
            raise Exception("invalid value for 'slave' in %s" % self.cfgfile)

        cmdline = [ "ssh", sshopt, host, self.script, slave_config, command ]

        if self.not_really:
            self.log.info("remote_walmgr: %s" % command)
        else:
            return self.exec_cmd(cmdline,allow_error)

    def walmgr_setup(self):
        if self.wtype == MASTER:
            self.log.info("Configuring WAL archiving")

            data_dir = self.cf.get("master_data")
            restart_cmd = self.cf.get("master_restart_cmd", "")

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
            def mkdir(dir):
                if not os.path.exists(dir):
                    self.log.debug("Creating directory %s" % dir)
                    os.mkdir(dir)
            mkdir(self.cf.get("slave"))
            mkdir(self.cf.get("completed_wals"))
            mkdir(self.cf.get("partial_wals"))
            mkdir(self.cf.get("full_backup"))

            cf_backup = self.cf.get("config_backup", "")
            if cf_backup:
                mkdir(cf_backup)


    def master_periodic(self):
        """
        Run periodic command on master node. 

        We keep time using .walshipping.last file, so this has to be run before 
        set_last_complete()
        """

        self.assert_valid_role(MASTER)

        try:
            command_interval = self.cf.getint("command_interval", 0)
            periodic_command = self.cf.get("periodic_command", "")

            if periodic_command:
                check_file = os.path.join(self.cf.get("master_data"), ".walshipping.periodic")

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

            data_dir = self.cf.get("master_data")
            dst_loc = self.cf.get("full_backup")
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
            conf_dst_loc = self.cf.get("config_backup", "")
            if conf_dst_loc:
                master_conf_dir = os.path.dirname(self.cf.get("master_config"))
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
        self.assert_valid_role(SLAVE)
        if self.slave_lock_backups() != 0:
            self.log.error("Cannot obtain backup lock.")
            sys.exit(1)

        try:
            self.slave_pause(waitcomplete=1)

            try:
                self.slave_rotate_backups()
                src = self.cf.get("slave_data")
                dst = self.cf.get("full_backup")

                start_time = time.localtime()
                cmdline = ["cp", "-a", src, dst ]
                self.log.info("Executing %s" % " ".join(cmdline))
                if not self.not_really:
                    self.exec_cmd(cmdline)
                stop_time = time.localtime()

                # Obtain the last restart point information
                ctl = PgControlData(self.cf.get("slave_bin", ""), dst, False)

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
                    completed_wals = self.cf.get("completed_wals")
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
        if self.wtype == MASTER:
            self.master_backup()
        else:
            self.slave_backup()

    def master_xarchive(self):
        """Copy a complete WAL segment to slave."""

        self.assert_valid_role(MASTER)

        if len(self.args) < 2:
            die(1, "usage: xarchive srcpath srcname")
        srcpath = self.args[0]
        srcname = self.args[1]

        start_time = time.time()
        self.log.debug("%s: start copy", srcname)
        
        self.master_periodic()
        self.set_last_complete(srcname)
        
        dst_loc = self.cf.get("completed_wals")
        if dst_loc[-1] != "/":
            dst_loc += "/"

        # copy data
        self.exec_rsync([ srcpath, dst_loc ], True)

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

        self.assert_valid_role(SLAVE)
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

        name = os.path.join(self.cf.get("partial_wals"), filename)
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
        self.assert_valid_role(MASTER)
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

        self.assert_valid_role(MASTER)

        use_xlog_functions = self.cf.getint("use_xlog_functions", False)
        data_dir = self.cf.get("master_data")
        xlog_dir = os.path.join(data_dir, "pg_xlog")
        master_bin = self.cf.get("master_bin", "")

        dst_loc = os.path.join(self.cf.get("partial_wals"), "")

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
                if self.exec_rsync([xlog, dst_loc]) != 0:
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
        if self.wtype == MASTER:
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
                time.sleep(10)
                self.log.info("Re-exec: %s", repr(sys.argv))
                os.execv(sys.argv[0], sys.argv)

    def master_xrestore(self, srcname, dstpath):
        """
        Restore the xlog file from slave.
        """
        paths = [ self.cf.get("completed_wals"), self.cf.get("partial_wals") ]
        
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
        srcdir = self.cf.get("completed_wals")
        partdir = self.cf.get("partial_wals")
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

        if self.wtype == SLAVE:
            data_dir = self.cf.get("slave_data")
            stop_cmd = self.cf.get("slave_stop_cmd", "")
            start_cmd = self.cf.get("slave_start_cmd")
            pidfile = os.path.join(data_dir, "postmaster.pid")
        else:
            if not setname or not altdst:
                die(1, "Source and target directories must be specified if running on master node.")
            data_dir = altdst
            stop_cmd = None
            pidfile = None

        if setname:
            full_dir = os.path.join(self.cf.get("slave"), setname)
        else:
            full_dir = self.cf.get("full_backup")

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

        if self.wtype == MASTER:
            print >>sys.stderr, "About to restore to directory %s. The postgres cluster should be shut down." % data_dir
            if not yesno("Is postgres shut down on %s ?" % data_dir):
                die(1, "Shut it down and try again.")

        if self.wtype == SLAVE:
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

        if self.cf.get('slave_pg_xlog', ''):
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
                   os.symlink(self.cf.get('slave_pg_xlog'), "%s/pg_xlog" % data_dir)

                if (self.wtype == MASTER and createbackup and os.path.isdir(bak)):
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
        ctl = PgControlData(self.cf.get("slave_bin", ""), data_dir, True)
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
            conf += "trigger_file = '%s'\n" % os.path.join(self.cf.get("completed_wals"), "STOP")
            conf += "primary_conninfo = '%s'\n" % primary_conninfo

        self.log.info("Write %s" % rconf)
        if self.not_really:
            print conf
        else:
            f = open(rconf, "w")
            f.write(conf)
            f.close()

        # remove stopfile on slave
        if self.wtype == SLAVE:
            stopfile = os.path.join(self.cf.get("completed_wals"), "STOP")
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
        self.assert_valid_role(SLAVE)

        cf_source_dir = self.cf.get("config_backup", "")
        cf_target_dir = self.cf.get("slave_config_dir", "")

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
        self.assert_valid_role(SLAVE)

        srcdir = self.cf.get("completed_wals")
        datadir = self.cf.get("slave_data")
        stopfile = os.path.join(srcdir, "STOP")

        if self.not_really:
            self.log.info("Writing STOP file: %s" % stopfile)
        else:
            open(stopfile, "w").write("1")
        self.log.info("Stopping recovery mode")


    def slave_pause(self, waitcomplete=0):
        """Pause the WAL apply, wait until last file applied if needed"""
        self.assert_valid_role(SLAVE)
        srcdir = self.cf.get("completed_wals")
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
        self.assert_valid_role(SLAVE)
        srcdir = self.cf.get("completed_wals")
        pausefile = os.path.join(srcdir, "PAUSE")
        if os.path.isfile(pausefile):
            if not self.not_really:
                os.remove(pausefile)
            self.log.info("Continuing with recovery")
        else:
            self.log.info("Recovery not paused?")

    def slave_lock_backups_exit(self):
        """Exit with lock acquired status"""
        self.assert_valid_role(SLAVE)
        sys.exit(self.slave_lock_backups())

    def slave_lock_backups(self):
        """Create lock file to deny other concurrent backups"""
        srcdir = self.cf.get("completed_wals")
        lockfile = os.path.join(srcdir, "BACKUPLOCK")
        if os.path.isfile(lockfile):
            self.log.warning("Somebody already has the backup lock.")
            lockfilehandle = open(lockfile,"r")
            pidstring = lockfilehandle.read();
            try:
                pid = int(pidstring)
                print("%d",pid)
            except ValueError:
                self.log.error("lock file does not contain a pid:" + pidstring)
            return 1

        if not self.not_really:
            open(lockfile, "w").write(self.args[0])
        self.log.info("Backup lock obtained.")
        return 0

    def slave_resume_backups(self):
        """Remove backup lock file, allow other backups to run"""
        self.assert_valid_role(SLAVE)
        srcdir = self.cf.get("completed_wals")
        lockfile = os.path.join(srcdir, "BACKUPLOCK")
        if os.path.isfile(lockfile):
            if not self.not_really:
                os.remove(lockfile)
            self.log.info("Backup lock released.")
        else:
            self.log.info("Backup lock not held.")

    def list_backups(self):
        """List available backups. On master this just calls slave listbackups via SSH"""
        if self.wtype == MASTER:
            self.remote_walmgr("listbackups")
        else:
            backups = self.get_backup_list(self.cf.get("full_backup"))
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
        slave_data = self.cf.get("slave_data")
        backup_label = os.path.join(slave_data, "backup_label")
        if os.path.exists(backup_label):
            # Label file still exists, use it for determining the restart point
            lbl = BackupLabel(slave_data)
            self.log.debug("Last restart point from backup_label: %s" % lbl.first_wal)
            return lbl.first_wal

        ctl = PgControlData(self.cf.get("slave_bin", ""), ".", True)
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
        self.assert_valid_role(SLAVE)
        backups = self.get_backup_list(self.cf.get("full_backup"))
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
        self.assert_valid_role(SLAVE)
        dst_loc = self.cf.get("full_backup")
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
        completed_wals = self.cf.get("completed_wals")
        partial_wals = self.cf.get("partial_wals")

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
                    os.remove(full)
            cur_last = fname
        return cur_last
    def remote_xlock(self):
        ret = self.remote_walmgr("xlock " + str(os.getpid()),allow_error=True)
        if ret[0] != 0:
            # lock failed.
            try:
                lock_pid = int(ret[1])
                if os.kill(lock_pid,0):
                    #process exists.
                    self.log.error("lock already obtained")
                else:
                    self.remote_walmgr("xrelease")
                    ret = self.remote_walmgr("xlock " + pid(),allow_error=True)
                    if ret[0] != 0:
                        self.log.error("unable to obtain lock")
            except ValueError:
                self.log.error("error obtaining lock")

if __name__ == "__main__":
    script = WalMgr(sys.argv[1:])
    script.start()
