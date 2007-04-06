"""PgQ maintenance functions."""

import skytools, time

def get_pgq_api_version(curs):
    q = "select count(1) from pg_proc p, pg_namespace n"\
        " where n.oid = p.pronamespace and n.nspname='pgq'"\
        "   and p.proname='version';"
    curs.execute(q)
    if not curs.fetchone()[0]:
        return '1.0.0'

    curs.execute("select pgq.version()")
    return curs.fetchone()[0]

def version_ge(curs, want_ver):
    """Check is db version of pgq is greater than want_ver."""
    db_ver = get_pgq_api_version(curs)
    want_tuple = map(int, want_ver.split('.'))
    db_tuple = map(int, db_ver.split('.'))
    if db_tuple[0] != want_tuple[0]:
        raise Exception('Wrong major version')
    if db_tuple[1] >= want_tuple[1]:
        return 1
    return 0

class MaintenanceJob(skytools.DBScript):
    """Periodic maintenance."""
    def __init__(self, ticker, args):
        skytools.DBScript.__init__(self, 'pgqadm', args)
        self.ticker = ticker
        self.last_time = 0 # start immidiately
        self.last_ticks = 0
        self.clean_ticks = 1
        self.maint_delay = 5*60

    def startup(self):
        # disable regular DBScript startup()
        pass

    def reload(self):
        skytools.DBScript.reload(self)

        # force loop_delay
        self.loop_delay = 5

        # compat var
        self.maint_delay = 60 * self.cf.getfloat('maint_delay_min', -1)
        if self.maint_delay < 0:
            self.maint_delay = self.cf.getfloat('maint_delay', 5*60)
        self.maint_delay = self.cf.getfloat('maint_delay', self.maint_delay)

    def work(self):
        t = time.time()
        if self.last_time + self.maint_delay > t:
            return

        self.do_maintenance()

        self.last_time = t
        duration = time.time() - t
        self.stat_add('maint_duration', duration)

    def do_maintenance(self):
        """Helper function for running maintenance."""

        db = self.get_database('db', autocommit=1)
        cx = db.cursor()

        if skytools.exists_function(cx, "pgq.maint_rotate_tables_step1", 1):
            # rotate each queue in own TX
            q = "select queue_name from pgq.get_queue_info()"
            cx.execute(q)
            for row in cx.fetchall():
                cx.execute("select pgq.maint_rotate_tables_step1(%s)", [row[0]])
                res = cx.fetchone()[0]
                if res:
                    self.log.info('Rotating %s' % row[0])
        else:
            cx.execute("select pgq.maint_rotate_tables_step1();")

        # finish rotation
        cx.execute("select pgq.maint_rotate_tables_step2();")

        # move retry events to main queue in small blocks
        rcount = 0
        while 1:
            cx.execute('select pgq.maint_retry_events();')
            res = cx.fetchone()[0]
            rcount += res
            if res == 0:
                break
        if rcount:
            self.log.info('Got %d events for retry' % rcount)

        # vacuum tables that are needed
        cx.execute('set maintenance_work_mem = 32768')
        cx.execute('select * from pgq.maint_tables_to_vacuum()')
        for row in cx.fetchall():
            cx.execute('vacuum %s;' % row[0])


