#! /usr/bin/env python

"""Bulk start/stop of scripts.

Reads a bunch of config files and maps them to scripts, then handles those.

Config template:

    [scriptmgr]
    job_name = scriptmgr_cphdb5
    config_list = ~/random/conf/*.ini
    logfile = ~/log/%(job_name)s.log
    pidfile = ~/pid/%(job_name)s.pid
    #use_skylog = 1

    # defaults for services
    [DEFAULT]
    cwd = ~/
    args = -v

    # service descriptions

    [cube_dispatcher]
    script = cube_dispatcher.py

    [table_dispatcher]
    script = table_dispatcher.py

    [bulk_loader]
    script = bulk_loader.py

    [londiste]
    script = londiste.py
    args = replay

    [pgqadm]
    script = pgqadm.py
    args = ticker

    # services to be ignored

    [log_checker]
    disabled = 1
"""

import sys, os, signal, glob, ConfigParser, time

import pkgloader
pkgloader.require('skytools', '3.0')
import skytools

try:
    import pwd
except ImportError:
    pwd = None

command_usage = """
%prog [options] INI CMD [subcmd args]

Commands:
  start -a | -t=service | jobname [...]    start job(s)
  stop -a | -t=service | jobname [...]     stop job(s)
  restart -a | -t=service | jobname [...]  restart job(s)
  reload -a | -t=service | jobname [...]   send reload signal
  status [-a | -t=service | jobname ...]
"""

def job_sort_cmp(j1, j2):
    d1 = j1['service'] + j1['job_name']
    d2 = j2['service'] + j2['job_name']
    if d1 < d2: return -1
    elif d1 > d2: return 1
    else: return 0

def launch_cmd(job, cmd):
    if job['user']:
        cmd = 'sudo -nH -u "%s" %s' % (job['user'], cmd)
    return os.system(cmd)

def full_path(job, fn):
    """Like os.path.expanduser() but works for other users.
    """
    if not fn:
        return fn
    if fn[0] == '~':
        if fn.find('/') > 0:
            user, rest = fn.split('/',1)
        else:
            user = fn
            rest = ''

        user = user[1:]
        if not user:
            user = job['user']

        # find home
        if user:
            home = pwd.getpwuid(os.getuid()).pw_dir
        elif 'HOME' in os.environ:
            home = os.environ['HOME']
        else:
            home = os.pwd.getpwuid(os.getuid()).pw_dir

        if rest:
            return os.path.join(home, rest)
        else:
            return home
    # always return full path
    return os.path.join(job['cwd'], fn)

class ScriptMgr(skytools.DBScript):
    __doc__ = __doc__
    svc_list = []
    svc_map = {}
    config_list = []
    job_map = {}
    job_list = []
    def init_optparse(self, p = None):
        p = skytools.DBScript.init_optparse(self, p)
        p.add_option("-a", "--all", action="store_true", help="apply command to all jobs")
        p.add_option("-t", "--type", action="store", metavar="SVC", help="apply command to all jobs of this service type")
        p.add_option("-w", "--wait", action="store_true", help="wait for job(s) after signaling")
        p.set_usage(command_usage.strip())
        return p

    def load_jobs(self):
        self.svc_list = []
        self.svc_map = {}
        self.config_list = []

        # load services
        svc_list = self.cf.sections()
        svc_list.remove(self.service_name)
        with_user = 0
        without_user = 0
        for svc_name in svc_list:
            cf = self.cf.clone(svc_name)
            disabled = cf.getboolean('disabled', 0)
            defscript = None
            if disabled:
                defscript = '/disabled'
            svc = {
                'service': svc_name,
                'script': cf.getfile('script', defscript),
                'cwd': cf.getfile('cwd'),
                'disabled': disabled,
                'args': cf.get('args', ''),
                'user': cf.get('user', ''),
            }
            if svc['user']:
                with_user += 1
            else:
                without_user += 1
            self.svc_list.append(svc)
            self.svc_map[svc_name] = svc
        if with_user and without_user:
            raise skytools.UsageError("Invalid config - some jobs have user=, some don't")

        # generate config list
        for tmp in self.cf.getlist('config_list'):
            tmp = os.path.expanduser(tmp)
            tmp = os.path.expandvars(tmp)
            for fn in glob.glob(tmp):
                self.config_list.append(fn)

        # read jobs
        for fn in self.config_list:
            raw = ConfigParser.SafeConfigParser({'job_name':'?', 'service_name':'?'})
            raw.read(fn)

            # skip its own config
            if raw.has_section(self.service_name):
                continue

            got = 0
            for sect in raw.sections():
                if sect in self.svc_map:
                    got = 1
                    self.add_job(fn, sect)
            if not got:
                self.log.warning('Cannot find service for %s' % fn)

    def add_job(self, cf_file, service_name):
        svc = self.svc_map[service_name]
        cf = skytools.Config(service_name, cf_file)
        disabled = svc['disabled']
        if not disabled:
            disabled = cf.getboolean('disabled', 0)
        job = {
            'disabled': disabled,
            'config': cf_file,
            'cwd': svc['cwd'],
            'script': svc['script'],
            'args': svc['args'],
            'user': svc['user'],
            'service': svc['service'],
            'job_name': cf.get('job_name'),
            'pidfile': cf.get('pidfile', ''),
        }

        if job['pidfile']:
            job['pidfile'] = full_path(job, job['pidfile'])

        self.job_list.append(job)
        self.job_map[job['job_name']] = job

    def cmd_status (self, jobs):
        for jn in jobs:
            try:
                job = self.job_map[jn]
            except KeyError:
                self.log.error ("Unknown job: %s", jn)
                continue
            pidfile = job['pidfile']
            name = job['job_name']
            svc = job['service']
            if job['disabled']:
                name += "  (disabled)"

            if not pidfile:
                print(" pidfile? [%s] %s" % (svc, name))
            elif os.path.isfile(pidfile):
                print(" OK       [%s] %s" % (svc, name))
            else:
                print(" STOPPED  [%s] %s" % (svc, name))

    def cmd_info (self, jobs):
        for jn in jobs:
            try:
                job = self.job_map[jn]
            except KeyError:
                self.log.error ("Unknown job: %s", jn)
                continue
            print(job)

    def cmd_start(self, job_name):
        job = self.get_job_by_name (job_name)
        if isinstance (job, int):
            return job # ret.code
        self.log.info('Starting %s' % job_name)
        pidfile = job['pidfile']
        if not pidfile:
            self.log.warning("No pidfile for %s, cannot launch" % job_name)
            return 0
        if os.path.isfile(pidfile):
            if skytools.signal_pidfile(pidfile, 0):
                self.log.warning("Script %s seems running" % job_name)
                return 0
            else:
                self.log.info("Ignoring stale pidfile for %s" % job_name)
        os.chdir(job['cwd'])
        cmd = "%(script)s %(config)s %(args)s -d" % job
        res = launch_cmd(job, cmd)
        self.log.debug(res)
        if res != 0:
            self.log.error('startup failed: %s' % job_name)
            return 1
        else:
            return 0

    def cmd_stop(self, job_name):
        job = self.get_job_by_name (job_name)
        if isinstance (job, int):
            return job # ret.code
        self.log.info('Stopping %s' % job_name)
        self.signal_job(job, signal.SIGINT)

    def cmd_reload(self, job_name):
        job = self.get_job_by_name (job_name)
        if isinstance (job, int):
            return job # ret.code
        self.log.info('Reloading %s' % job_name)
        self.signal_job(job, signal.SIGHUP)

    def get_job_by_name (self, job_name):
        if job_name not in self.job_map:
            self.log.error ("Unknown job: %s" % job_name)
            return 1
        job = self.job_map[job_name]
        if job['disabled']:
            self.log.info ("Skipping %s" % job_name)
            return 0
        return job

    def wait_for_stop (self, job_name):
        job = self.get_job_by_name (job_name)
        if isinstance (job, int):
            return job # ret.code
        msg = False
        while True:
            if skytools.signal_pidfile (job['pidfile'], 0):
                if not msg:
                    self.log.info ("Waiting for %s to stop" % job_name)
                    msg = True
                time.sleep (0.1)
            else:
                return 0

    def signal_job(self, job, sig):
        pidfile = job['pidfile']
        if not pidfile:
            self.log.warning("No pidfile for %s (%s)" % (job['job_name'], job['config']))
            return
        if os.path.isfile(pidfile):
            pid = int(open(pidfile).read())
            if job['user']:
                # run sudo + kill to avoid killing unrelated processes
                res = os.system("sudo -u %s kill %d" % (job['user'], pid))
                if res:
                    self.log.warning("Signaling %s failed" % (job['job_name'],))
            else:
                # direct kill
                try:
                    os.kill(pid, sig)
                except Exception, det:
                    self.log.warning("Signaling %s failed: %s" % (job['job_name'], str(det)))
        else:
            self.log.warning("Job %s not running" % job['job_name'])

    def work(self):
        self.set_single_loop(1)
        self.job_list = []
        self.job_map = {}
        self.load_jobs()
        self.job_list.sort(job_sort_cmp)

        if len(self.args) < 2:
            print("need command")
            sys.exit(1)

        cmd = self.args[1]
        jobs = self.args[2:]

        if cmd in ["status", "info"] and len(jobs) == 0 and not self.options.type:
            self.options.all = True

        if len(jobs) == 0 and self.options.all:
            for job in self.job_list:
                jobs.append(job['job_name'])
        if len(jobs) == 0 and self.options.type:
            for job in self.job_list:
                if job['service'] == self.options.type:
                    jobs.append(job['job_name'])

        if cmd == "status":
            self.cmd_status(jobs)
            return
        elif cmd == "info":
            self.cmd_info(jobs)
            return

        if len(jobs) == 0:
            print("no jobs given?")
            sys.exit(1)

        if cmd == "start":
            err = 0
            for n in jobs:
                err += self.cmd_start(n)
            if err > 0:
                self.log.error('some scripts failed')
                sys.exit(1)
        elif cmd == "stop":
            for n in jobs:
                self.cmd_stop(n)
            if self.options.wait:
                for n in jobs:
                    self.wait_for_stop(n)
        elif cmd == "restart":
            for n in jobs:
                self.cmd_stop(n)
            if self.options.wait:
                for n in jobs:
                    self.wait_for_stop(n)
            else:
                time.sleep(2)
            for n in jobs:
                self.cmd_start(n)
        elif cmd == "reload":
            for n in jobs:
                self.cmd_reload(n)
        else:
            print("unknown command: " + cmd)
            sys.exit(1)

if __name__ == '__main__':
    script = ScriptMgr('scriptmgr', sys.argv[1:])
    script.start()
