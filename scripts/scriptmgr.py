#! /usr/bin/env python

"""Bulk start/stop of scripts.

Reads a bunch of config files and maps them to scripts, then handles those.
"""

import sys, os, skytools, signal, glob, ConfigParser, time

command_usage = """
%prog [options] INI CMD [subcmd args]

commands:
  start [-a | jobname ..]    start a job
  stop [-a | jobname ..]     stop a job
  restart [-a | jobname ..]  restart job(s)
  reload [-a | jobname ..]   send reload signal
  status
"""

def job_sort_cmp(j1, j2):
    d1 = j1['service'] + j1['job_name']
    d2 = j2['service'] + j2['job_name']
    if d1 < d2: return -1
    elif d1 > d2: return 1
    else: return 0

class ScriptMgr(skytools.DBScript):
    def init_optparse(self, p = None):
        p = skytools.DBScript.init_optparse(self, p)
        p.add_option("-a", "--all", action="store_true", help="apply command to all jobs")
        p.set_usage(command_usage.strip())
        return p

    def load_jobs(self):
        self.svc_list = []
        self.svc_map = {}
        self.config_list = []

        # load services
        svc_list = self.cf.sections()
        svc_list.remove(self.service_name)
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
                'disabled': cf.getboolean('disabled', 0),
                'args': cf.get('args', ''),
            }
            self.svc_list.append(svc)
            self.svc_map[svc_name] = svc

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
            'service': svc['service'],
            'job_name': cf.get('job_name'),
            'pidfile': cf.getfile('pidfile', ''),
        }
        self.job_list.append(job)
        self.job_map[job['job_name']] = job

    def cmd_status(self):
        for job in self.job_list:
            os.chdir(job['cwd'])
            cf = skytools.Config(job['service'], job['config'])
            pidfile = cf.getfile('pidfile', '')
            name = job['job_name']
            svc = job['service']
            if job['disabled']:
                name += "  (disabled)"
            
            if not pidfile:
                print " pidfile? [%s] %s" % (svc, name)
            elif os.path.isfile(pidfile):
                print " OK       [%s] %s" % (svc, name)
            else:
                print " STOPPED  [%s] %s" % (svc, name)

    def cmd_info(self):
        for job in self.job_list:
            print job

    def cmd_start(self, job_name):
        if job_name not in self.job_map:
            self.log.error('Unknown job: '+job_name)
            return 1
        job = self.job_map[job_name]
        if job['disabled']:
            self.log.info("Skipping %s" % job_name)
            return 0
        self.log.info('Starting %s' % job_name)
        os.chdir(job['cwd'])
        pidfile = job['pidfile']
        if not pidfile:
            self.log.warning("No pidfile for %s cannot launch")
            return 0
        if os.path.isfile(pidfile):
            self.log.warning("Script %s seems running" % job_name)
            return 0
        cmd = "%(script)s %(config)s %(args)s -d" % job
        res = os.system(cmd)
        self.log.debug(res)
        if res != 0:
            self.log.error('startup failed: %s' % job_name)
            return 1
        else:
            return 0

    def cmd_stop(self, job_name):
        if job_name not in self.job_map:
            self.log.error('Unknown job: '+job_name)
            return
        job = self.job_map[job_name]
        if job['disabled']:
            self.log.info("Skipping %s" % job_name)
            return
        self.log.info('Stopping %s' % job_name)
        self.signal_job(job, signal.SIGINT)

    def cmd_reload(self, job_name):
        if job_name not in self.job_map:
            self.log.error('Unknown job: '+job_name)
            return
        job = self.job_map[job_name]
        if job['disabled']:
            self.log.info("Skipping %s" % job_name)
            return
        self.log.info('Reloading %s' % job_name)
        self.signal_job(job, signal.SIGHUP)

    def signal_job(self, job, sig):
        os.chdir(job['cwd'])
        pidfile = job['pidfile']
        if not pidfile:
            self.log.warning("No pidfile for %s (%s)" % (job['job_name'], job['config']))
            return
        if os.path.isfile(pidfile):
            pid = int(open(pidfile).read())
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

        if len(self.args) < 2:
            print "need command"
            sys.exit(1)

        jobs = self.args[2:]
        if len(jobs) == 0 and self.options.all:
            for job in self.job_list:
                jobs.append(job['job_name'])

        self.job_list.sort(job_sort_cmp)

        cmd = self.args[1]
        if cmd == "status":
            self.cmd_status()
            return
        elif cmd == "info":
            self.cmd_info()
            return

        if len(jobs) == 0:
            print "no jobs given?"
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
        elif cmd == "restart":
            for n in jobs:
                self.cmd_stop(n)
                time.sleep(2)
                self.cmd_start(n)
        elif cmd == "reload":
            for n in jobs:
                self.cmd_reload(n)
        else:
            print "unknown command:", cmd
            sys.exit(1)

if __name__ == '__main__':
    script = ScriptMgr('scriptmgr', sys.argv[1:])
    script.start()

