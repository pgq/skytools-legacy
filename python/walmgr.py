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

"""
Additional features:
 * Simplified install. Master "setup" command should setup slave directories.
 * Add support for multiple targets on master.
 * Add an optional time based WAL retention parameter, this could be useful if base backups
 are taken from the standby (8.2 only)
 * WAL purge does not correctly purge old WAL-s if timelines are involved. The first
 useful WAL name is obtained from backup_label, WAL-s in the same timeline that are older 
 than first useful WAL are removed. 
 * xrestore should not attempt to copy the file on disk full condition - this
 will result in recovery failure. Pre 8.2 this means starting from zero.
 * Always copy the directory on "restore" add a special "--move" option.
"""

import os, sys, skytools, re, signal, time, traceback
import errno, glob, ConfigParser

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

class WalChunk:
    def __init__(self,filename,pos=0,bytes=0):
        self.filename = filename
        self.pos = pos
        self.bytes = bytes
        self.start_time = time.time()
        self.sync_count = 0
        self.sync_time = 0.0

    def __str__(self):
        return "%s @ %d +%d" % (self.filename, self.pos, self.bytes)

class BackupLabel:
    def __init__(self):
        self.first_wal = None
        self.start_time = None
        self.label_string = None
        self.fromslave = None

def get_backup_label(dirname):
    label = BackupLabel()
    filename = os.path.join(dirname, "backup_label")
    if not os.path.exists(filename):
        # perhaps this is a backup taken from slave, try .old suffix
        filename += ".old"
        if not os.path.exists(filename):
            return None
        label.fromslave = True
    for line in open(filename):
        m = re.match('^START WAL LOCATION: [^\s]+ \(file ([0-9A-Z]+)\)$', line)
        if m:
            label.first_wal = m.group(1)
        m = re.match('^START TIME:\s(.*)$', line)
        if m:
            label.start_time = m.group(1)
        m = re.match('^LABEL: (.*)$', line)
        if m:
            label.label_string = m.group(1)
    return label

class WalMgr(skytools.DBScript):

    def init_optparse(self, parser=None):
        p = skytools.DBScript.init_optparse(self, parser)
        p.set_usage(__doc__.strip())
        p.add_option("-n", "--not-really", action="store_true", dest="not_really",
                     help = "Don't actually do anything.", default=False)
        return p

    def __init__(self, args):

        if len(args) > 0:
            # hack to determine the role of the node
            cf = ConfigParser.ConfigParser()
            cf.read(args[0])
            for (self.wtype, self.service_name) in [ (MASTER, "wal-master"), (SLAVE, "wal-slave") ]:
                if cf.has_section(self.service_name):
                    break

        skytools.DBScript.__init__(self, self.service_name, args)
        self.set_single_loop(1)

        self.not_really = self.options.not_really
        self.pg_backup = 0
        self.walchunk = None

        if len(self.args) < 2:
            usage(1)
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
            self.log.warning("Action not available on current node.");
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
            cmdline.append("-z");
        cmdline += args

        cmd = "' '".join(cmdline)
        self.log.debug("Execute rsync cmd: '%s'" % (cmd))
        if self.not_really:
            return
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

    def exec_cmd(self, cmdline):
        cmd = "' '".join(cmdline)
        self.log.debug("Execute cmd: '%s'" % (cmd))
        if self.not_really:
            return
        res = os.spawnvp(os.P_WAIT, cmdline[0], cmdline)
        if res != 0:
            self.log.fatal("exec failed, res=%d (%s)" % (res, repr(cmdline)))
            sys.exit(1)

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
        self.assert_valid_role(MASTER)
        self.log.info("Disabling WAL archiving")

        self.master_configure_archiving('')

    def master_configure_archiving(self, cf_val):
        cf_file = self.cf.get("master_config")
        data_dir = self.cf.get("master_data")
        r_active = re.compile("^[ ]*archive_command[ ]*=[ ]*'(.*)'.*$", re.M)
        r_disabled = re.compile("^.*archive_command.*$", re.M)

        cf_full = "archive_command = '%s'" % cf_val

        if not os.path.isfile(cf_file):
            self.log.fatal("Config file not found: %s" % cf_file)
        self.log.info("Using config file: %s", cf_file)

        buf = open(cf_file, "r").read()
        m = r_active.search(buf)
        if m:
            old_val = m.group(1)
            if old_val == cf_val:
                self.log.debug("postmaster already configured")
            else:
                self.log.debug("found active but different conf")
                newbuf = "%s%s%s" % (buf[:m.start()], cf_full, buf[m.end():])
                self.change_config(cf_file, newbuf)
        else:
            m = r_disabled.search(buf)
            if m:
                self.log.debug("found disabled value")
                newbuf = "%s\n%s%s" % (buf[:m.end()], cf_full, buf[m.end():])
                self.change_config(cf_file, newbuf)
            else:
                self.log.debug("found no value")
                newbuf = "%s\n%s\n\n" % (buf, cf_full)
                self.change_config(cf_file, newbuf)

        self.log.info("Sending SIGHUP to postmaster")
        self.signal_postmaster(data_dir, signal.SIGHUP)
        self.log.info("Done")

    def change_config(self, cf_file, buf):
        cf_old = cf_file + ".old"
        cf_new = cf_file + ".new"

        if self.not_really:
            cf_new = "/tmp/postgresql.conf.new"
            open(cf_new, "w").write(buf)
            self.log.info("Showing diff")
            os.system("diff -u %s %s" % (cf_file, cf_new))
            self.log.info("Done diff")
            os.remove(cf_new)
            return

        # polite method does not work, as usually not enough perms for it
        if 0:
            open(cf_new, "w").write(buf)
            bak = open(cf_file, "r").read()
            open(cf_old, "w").write(bak)
            os.rename(cf_new, cf_file)
        else:
            open(cf_file, "w").write(buf)

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

    def remote_walmgr(self, command, stdin_disabled = True):
        """Pass a command to slave WalManager"""
        slave = self.cf.get("slave")
        slave_config = self.cf.get("slave_config", "")
        tmp = slave.split(":", 1)

        sshopt = "-T"
        if stdin_disabled:
            sshopt += "n"

        cmdline = None

        if len(tmp) < 2:
            raise Exception("cannot find slave hostname")
        else:
            host, path = tmp
            cmdline = [ "ssh", sshopt, host, self.script  ]

        if slave_config:
            cmdline += [ slave_config ]
        cmdline += [ command ]

        if self.not_really:
            self.log.info("remote_walmgr: %s" % command)
        else:
            self.exec_cmd(cmdline)

    def walmgr_setup(self):
        if self.wtype == MASTER:
            self.log.info("Configuring WAL archiving")

            cf_file = os.path.abspath(self.cf.filename)
            cf_val = "%s %s %s" % (self.script, cf_file, "xarchive %p %f")

            self.master_configure_archiving(cf_val)
            # ask slave to init
            self.remote_walmgr("setup")
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
                        rc = os.WEXITSTATUS(os.system(periodic_command))
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

        self.remote_walmgr("xlock")
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
                    "--exclude", "pg_log/*",
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

            # copy pg_xlog
            self.chdir(data_dir)
            cmdline = [
                "--exclude", "*.done",
                "--exclude", "*.backup",
                "--copy-unsafe-links",
                "--delete", "pg_xlog", dst_loc]
            self.exec_big_rsync(cmdline)

            self.remote_walmgr("xpurgewals")
        except:
            errors = True

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
        6. Purge unneeded WAL-s
        7. Resume WAL apply
        8. Release backup lock
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
                cmdline = ["cp", "-a", src, dst ]
                self.log.info("Executing %s" % " ".join(cmdline))
                if not self.not_really:
                    self.exec_cmd(cmdline)
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

        status = os.waitpid(childpid, 0);
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
        dst_loc = self.cf.get("partial_wals")
        if dst_loc[-1] != "/":
            dst_loc += "/"

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

            for fn in files:
                # check if interesting file
                if len(fn) < 10:
                    continue
                if fn[0] < "0" or fn[0] > '9':
                    continue
                if fn.find(".") > 0:
                    continue
                # check if to old
                if last:
                    dot = last.find(".")
                    if dot > 0:
                        xlast = last[:dot]
                        if fn < xlast:
                            continue
                    else:
                        if fn <= last:
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
            die(1, "usage: xrestore srcname dstpath")
        srcname = self.args[0]
        dstpath = self.args[1]
        if self.wtype == MASTER:
            self.master_xrestore(srcname, dstpath)
        else:
            self.slave_xrestore_unsafe(srcname, dstpath)

    def slave_xrestore(self, srcname, dstpath):
        loop = 1
        while loop:
            try:
                self.slave_xrestore_unsafe(srcname, dstpath)
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

    def slave_xrestore_unsafe(self, srcname, dstpath):
        srcdir = self.cf.get("completed_wals")
        partdir = self.cf.get("partial_wals")
        pausefile = os.path.join(srcdir, "PAUSE")
        stopfile = os.path.join(srcdir, "STOP")
        prgrfile = os.path.join(srcdir, "PROGRESS")
        srcfile = os.path.join(srcdir, srcname)
        partfile = os.path.join(partdir, srcname)

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
            try:
                os.kill(os.getppid(), 0)
            except OSError, ex:
                if ex.errno == errno.ESRCH:
                    self.log.info("%s: not found, stopping" % srcname)
                    sys.exit(1)
                self.log.warning("Parent aliveness check failed: "+str(ex))

            # nothing to do, sleep
            self.log.debug("%s: not found, sleeping" % srcname)
            time.sleep(1)

        # got one, copy it
        cmdline = ["cp", srcfile, dstpath]
        self.exec_cmd(cmdline)

        if self.cf.getint("keep_backups", 0) == 0:
            # cleanup only if we don't keep backup history.
            # historic WAL files are removed during backup rotation
            self.log.debug("%s: copy done, cleanup" % srcname)
            self.slave_cleanup(srcname)

        if os.path.isfile(partfile) and not srcfile == partfile:
            # Remove any partial files after restore. Only leave the partial if
            # it is actually used in recovery.
            self.log.debug("%s: removing partial not anymore needed for recovery." % partfile)
            os.remove(partfile)

        # create a PROGRESS file to notify that postgres is processing the WAL
        open(prgrfile, "w").write("1")

        # it would be nice to have apply time too
        self.stat_add('count', 1)
        self.send_stats()

    def restore_database(self):
        """Restore the database from backup

        If setname is specified, the contents of that backup set directory are 
        restored instead of "full_backup". Also copy is used instead of rename to 
        restore the directory.

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
            if not self.not_really:
                os.system(stop_cmd)
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

        if not setname and os.path.isdir(data_dir):
            # compatibility mode - default restore on slave and data directory exists
            self.log.warning("Old data directory is in the way, gotta move it.")
            createbackup = True

        # move old data away
        if createbackup and os.path.isdir(data_dir):
            self.log.info("Move %s to %s" % (data_dir, bak))
            if not self.not_really:
                os.rename(data_dir, bak)

        # move new data, copy if setname specified
        self.log.info("%s %s to %s" % (setname and "Copy" or "Move", full_dir, data_dir))
        if not self.not_really:
            if not setname:
                os.rename(full_dir, data_dir)
            else:
                self.exec_rsync(["--delete", "--no-relative", "--exclude=pg_xlog/*", os.path.join(full_dir,""), data_dir], True)
                if self.wtype == MASTER and createbackup and os.path.isdir(bak):
                    # restore original xlog files to data_dir/pg_xlog   
                    # symlinked directories are dereferences
                    self.exec_cmd(["cp", "-rL", "%s/pg_xlog" % bak, data_dir])
        else:
            data_dir = full_dir

        # re-link tablespaces
        spc_dir = os.path.join(data_dir, "pg_tblspc")
        tmp_dir = os.path.join(data_dir, "tmpspc")
        if os.path.isdir(spc_dir) and os.path.isdir(tmp_dir):
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

        conf = "\nrestore_command = '%s %s %s'\n" % (self.script, cf_file, 'xrestore %f "%p"')
        conf += "#recovery_target_time=''\n" + \
                "#recovery_target_xid=''\n" + \
                "#recovery_target_inclusive=true\n" + \
                "#recovery_target_timeline=''\n"
        self.log.info("Write %s" % rconf)
        if self.not_really:
            print conf
        else:
            f = open(rconf, "w")
            f.write(conf)
            f.close()

        # remove stopfile on slave
        if self.wtype == SLAVE:
            srcdir = self.cf.get("completed_wals")
            stopfile = os.path.join(srcdir, "STOP")
            if os.path.isfile(stopfile):
                self.log.info("Removing stopfile: "+stopfile)
                if not self.not_really:
                    os.remove(stopfile)

            # run database in recovery mode
            self.log.info("Starting postmaster: " + start_cmd)
            if not self.not_really:
                os.system(start_cmd)
        else:
            self.log.info("Data files restored, recovery.conf created.")
            self.log.info("postgresql.conf and additional WAL files may need to be restored manually.")

    def slave_boot(self):
        self.assert_valid_role(SLAVE)

        srcdir = self.cf.get("completed_wals")
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
            return 1

        if not self.not_really:
            open(lockfile, "w").write("1")
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
                print "%-15s %-24s %-10s %-24s" % \
                    ("Backup set", "Timestamp", "Label", "First WAL")
                print "%s %s %s %s" % (15*'-', 24*'-', 10*'-',24*'-')
                for backup in backups:
                    lbl = get_backup_label(backup)
                    print "%-15s %-24.24s %-10.10s %-24s%s" % \
                        (os.path.basename(backup), lbl.start_time, lbl.label_string,
                        lbl.first_wal, lbl.fromslave and "*" or "")
                print
            else:
                print "\nNo backups found.\n"

    def get_first_walname(self,backupdir):
        """Returns the name of the first needed WAL segment for backupset"""
        label = get_backup_label(backupdir)
        if not label.first_wal:
            self.log.error("WAL name not found at %s" % backupdir)
            return None
        return label.first_wal

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
                if not self.not_really:
                    rc = os.WEXITSTATUS(os.system(cmdline))
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

        self.log.debug("cleaning completed wals since %s" % last_applied)
        last = self.del_wals(completed_wals, last_applied)
        if last:
            if os.path.isdir(partial_wals):
                self.log.debug("cleaning partial wals since %s" % last)
                self.del_wals(partial_wals, last)
            else:
                self.log.warning("partial_wals dir does not exist: %s"
                              % partial_wals)
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

if __name__ == "__main__":
    script = WalMgr(sys.argv[1:])
    script.start()
