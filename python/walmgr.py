#! /usr/bin/env python

"""WALShipping manager.

walmgr [-n] COMMAND

Master commands:
  setup       Configure PostgreSQL for WAL archiving
  backup      Copies all master data to slave
  sync        Copies in-progress WALs to slave
  syncdaemon  Daemon mode for regular syncing
  stop        Stop archiving - de-configure PostgreSQL

Slave commands:
  restore     Stop postmaster, move new data dir to right
              location and start postmaster in playback mode.
  boot        Stop playback, accept queries.
  pause       Just wait, don't play WAL-s
  continue    Start playing WAL-s again

Internal commands:
  xarchive    archive one WAL file (master)
  xrestore    restore one WAL file (slave)

Switches:
  -n          no action, just print commands
"""

import os, sys, skytools, getopt, re, signal, time, traceback

MASTER = 1
SLAVE = 0

def usage(err):
    if err > 0:
        print >>sys.stderr, __doc__
    else:
        print __doc__
    sys.exit(err)

class WalMgr(skytools.DBScript):
    def __init__(self, wtype, cf_file, not_really, internal = 0, go_daemon = 0):
        self.not_really = not_really
        self.pg_backup = 0

        if wtype == MASTER:
            service_name = "wal-master"
        else:
            service_name = "wal-slave"

        if not os.path.isfile(cf_file):
            print "Config not found:", cf_file
            sys.exit(1)

        if go_daemon:
            s_args = ["-d", cf_file]
        else:
            s_args = [cf_file]

        skytools.DBScript.__init__(self, service_name, s_args,
                                    force_logfile = internal)

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
            self.log.info("postmaster is not running")
            return
        buf = open(pidfile, "r").readline()
        pid = int(buf.strip())
        self.log.debug("Signal %d to process %d" % (sgn, pid))
        if not self.not_really:
            os.kill(pid, sgn)

    def exec_big_rsync(self, cmdline):
        cmd = "' '".join(cmdline)
        self.log.debug("Execute big rsync cmd: '%s'" % (cmd))
        if self.not_really:
            return
        res = os.spawnvp(os.P_WAIT, cmdline[0], cmdline)
        if res == 24:
            self.log.info("Some files vanished, but thats OK")
        elif res != 0:
            self.log.fatal("exec failed, res=%d" % res)
            self.pg_stop_backup()
            sys.exit(1)

    def exec_cmd(self, cmdline):
        cmd = "' '".join(cmdline)
        self.log.debug("Execute cmd: '%s'" % (cmd))
        if self.not_really:
            return
        res = os.spawnvp(os.P_WAIT, cmdline[0], cmdline)
        if res != 0:
            self.log.fatal("exec failed, res=%d" % res)
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

    def master_setup(self):
        self.log.info("Configuring WAL archiving")

        script = os.path.abspath(sys.argv[0])
        cf_file = os.path.abspath(self.cf.filename)
        cf_val = "%s %s %s" % (script, cf_file, "xarchive %p %f")

        self.master_configure_archiving(cf_val)

    def master_stop(self):
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
        if len(tmp) != 2:
            raise Exception("cannot find hostname")
        host, path = tmp
        cmdline = ["ssh", host, "mkdir", "-p", path]
        self.exec_cmd(cmdline)

    def master_backup(self):
        """Copy master data directory to slave."""

        data_dir = self.cf.get("master_data")
        dst_loc = self.cf.get("full_backup")
        if dst_loc[-1] != "/":
            dst_loc += "/"

        self.pg_start_backup("FullBackup")

        master_spc_dir = os.path.join(data_dir, "pg_tblspc")
        slave_spc_dir = dst_loc + "tmpspc"

        # copy data
        self.chdir(data_dir)
        cmdline = ["rsync", "-a", "--delete",
                "--exclude", ".*",
                "--exclude", "*.pid",
                "--exclude", "*.opts",
                "--exclude", "*.conf",
                "--exclude", "*.conf.*",
                "--exclude", "pg_xlog",
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
                cmdline = ["rsync", "-a", "--delete",
                                    "--exclude", ".*",
                                    ".", dstfn]
                self.exec_big_rsync(cmdline)

        # copy pg_xlog
        self.chdir(data_dir)
        cmdline = ["rsync", "-a",
            "--exclude", "*.done",
            "--exclude", "*.backup",
            "--delete", "pg_xlog", dst_loc]
        self.exec_big_rsync(cmdline)

        self.pg_stop_backup()

        self.log.info("Full backup successful")

    def master_xarchive(self, srcpath, srcname):
        """Copy a complete WAL segment to slave."""

        start_time = time.time()
        self.log.debug("%s: start copy", srcname)
        
        self.set_last_complete(srcname)
        
        dst_loc = self.cf.get("completed_wals")
        if dst_loc[-1] != "/":
            dst_loc += "/"

        # copy data
        cmdline = ["rsync", "-t", srcpath, dst_loc]
        self.exec_cmd(cmdline)

        self.log.debug("%s: done", srcname)
        end_time = time.time()
        self.stat_add('count', 1)
        self.stat_add('duration', end_time - start_time)

    def master_sync(self):
        """Copy partial WAL segments."""
        
        data_dir = self.cf.get("master_data")
        xlog_dir = os.path.join(data_dir, "pg_xlog")
        dst_loc = self.cf.get("partial_wals")
        if dst_loc[-1] != "/":
            dst_loc += "/"

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
            cmdline = ["rsync", "-t", xlog, dst_loc]
            self.exec_cmd(cmdline)

        self.log.info("Partial copy done")

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

    def slave_xrestore_unsafe(self, srcname, dstpath):
        srcdir = self.cf.get("completed_wals")
        partdir = self.cf.get("partial_wals")
        keep_old_logs = self.cf.getint("keep_old_logs", 0)
        pausefile = os.path.join(srcdir, "PAUSE")
        stopfile = os.path.join(srcdir, "STOP")
        srcfile = os.path.join(srcdir, srcname)
        partfile = os.path.join(partdir, srcname)

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

            # nothing to do, sleep
            self.log.debug("%s: not found, sleeping" % srcname)
            time.sleep(20)

        # got one, copy it
        cmdline = ["cp", srcfile, dstpath]
        self.exec_cmd(cmdline)

        self.log.debug("%s: copy done, cleanup" % srcname)
        self.slave_cleanup(srcname)

        # it would be nice to have apply time too
        self.stat_add('count', 1)

    def slave_startup(self):
        data_dir = self.cf.get("slave_data")
        full_dir = self.cf.get("full_backup")
        stop_cmd = self.cf.get("slave_stop_cmd", "")
        start_cmd = self.cf.get("slave_start_cmd")
        pidfile = os.path.join(data_dir, "postmaster.pid")

        # stop postmaster if ordered
        if stop_cmd and os.path.isfile(pidfile):
            self.log.info("Stopping postmaster: " + stop_cmd)
            if not self.not_really:
                os.system(stop_cmd)
                time.sleep(3)

        # is it dead?
        if os.path.isfile(pidfile):
            self.log.fatal("Postmaster still running.  Cannot continue.")
            sys.exit(1)

        # find name for data backup
        i = 0
        while 1:
            bak = "%s.%d" % (data_dir, i)
            if not os.path.isdir(bak):
                break
            i += 1

        # move old data away
        if os.path.isdir(data_dir):
            self.log.info("Move %s to %s" % (data_dir, bak))
            if not self.not_really:
                os.rename(data_dir, bak)

        # move new data
        self.log.info("Move %s to %s" % (full_dir, data_dir))
        if not self.not_really:
            os.rename(full_dir, data_dir)
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
                link_loc = os.path.join(spc_dir, d)
                link_dst = os.path.join(tmp_dir, d)
                self.log.info("Linking tablespace %s to %s" % (d, link_dst))
                if not self.not_really:
                    if os.path.islink(link_loc):
                        os.remove(link_loc)
                    os.symlink(link_dst, link_loc)

        # write recovery.conf
        rconf = os.path.join(data_dir, "recovery.conf")
        script = os.path.abspath(sys.argv[0])
        cf_file = os.path.abspath(self.cf.filename)
        conf = "\nrestore_command = '%s %s %s'\n" % (
                script, cf_file, 'xrestore %f "%p"')
        self.log.info("Write %s" % rconf)
        if self.not_really:
            print conf
        else:
            f = open(rconf, "w")
            f.write(conf)
            f.close()

        # remove stopfile
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

    def slave_boot(self):
        srcdir = self.cf.get("completed_wals")
        stopfile = os.path.join(srcdir, "STOP")
        open(stopfile, "w").write("1")
        self.log.info("Stopping recovery mode")

    def slave_pause(self):
        srcdir = self.cf.get("completed_wals")
        pausefile = os.path.join(srcdir, "PAUSE")
        open(pausefile, "w").write("1")
        self.log.info("Pausing recovery mode")

    def slave_continue(self):
        srcdir = self.cf.get("completed_wals")
        pausefile = os.path.join(srcdir, "PAUSE")
        if os.path.isfile(pausefile):
            os.remove(pausefile)
            self.log.info("Continuing with recovery")
        else:
            self.log.info("Recovery not paused?")

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

            ok_del = 0
            if fname < last:
                self.log.debug("deleting %s" % full)
                os.remove(full)
            cur_last = fname
        return cur_last

    def work(self):
        self.master_sync()

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "nh")
    except getopt.error, det:
        print det
        usage(1)
    not_really = 0
    for o, v in opts:
        if o == "-n":
            not_really = 1
        elif o == "-h":
            usage(0)
    if len(args) < 2:
        usage(1)
    ini = args[0]
    cmd = args[1]

    if cmd == "setup":
        script = WalMgr(MASTER, ini, not_really)
        script.master_setup()
    elif cmd == "stop":
        script = WalMgr(MASTER, ini, not_really)
        script.master_stop()
    elif cmd == "backup":
        script = WalMgr(MASTER, ini, not_really)
        script.master_backup()
    elif cmd == "xarchive":
        if len(args) != 4:
            print >> sys.stderr, "usage: walmgr INI xarchive %p %f"
            sys.exit(1)
        script = WalMgr(MASTER, ini, not_really, 1)
        script.master_xarchive(args[2], args[3])
    elif cmd == "sync":
        script = WalMgr(MASTER, ini, not_really)
        script.master_sync()
    elif cmd == "syncdaemon":
        script = WalMgr(MASTER, ini, not_really, go_daemon=1)
        script.start()
    elif cmd == "xrestore":
        if len(args) != 4:
            print >> sys.stderr, "usage: walmgr INI xrestore %p %f"
            sys.exit(1)
        script = WalMgr(SLAVE, ini, not_really, 1)
        script.slave_xrestore(args[2], args[3])
    elif cmd == "restore":
        script = WalMgr(SLAVE, ini, not_really)
        script.slave_startup()
    elif cmd == "boot":
        script = WalMgr(SLAVE, ini, not_really)
        script.slave_boot()
    elif cmd == "pause":
        script = WalMgr(SLAVE, ini, not_really)
        script.slave_pause()
    elif cmd == "continue":
        script = WalMgr(SLAVE, ini, not_really)
        script.slave_continue()
    else:
        usage(1)

if __name__ == '__main__':
    main()

