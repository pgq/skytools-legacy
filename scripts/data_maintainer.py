#!/usr/bin/env python

"""Generic script for processing large data sets in small batches.

Reads events from one datasource and commits them into another one,
either one by one or in batches.

Config template::

    [data_maintainer3]
    job_name        = dm_remove_expired_services

    dbread          = dbname=sourcedb_test
    dbwrite         = dbname=destdb port=1234 host=dbhost.com user=guest password=secret
    dbbefore        = dbname=destdb_test
    dbafter         = dbname=destdb_test
    dbcrash         = dbname=destdb_test

    sql_get_pk_list =
        select username
        from user_service
        where expire_date < now();

    # It is a good practice to include same where condition on target side as on read side,
    # to ensure that you are actually changing the same data you think you are,
    # especially when reading from replica database or when processing takes days.
    sql_modify =
        delete from user_service
        where username = %%(username)s
        and expire_date < now();

    # This will be run before executing the sql_get_pk_list query (optional)
    sql_before_run =
        select * from somefunction1(%(job_name)s);

    # This will be run when the DM finishes (optional)
    sql_after_run =
        select * from somefunction2(%(job_name)s);

    # Determines whether the sql_after_run query will be run in case the pk list query returns no rows
    after_zero_rows = 1

    # This will be run if the DM crashes (optional)
    sql_on_crash =
        select * from somefunction3(%(job_name)s);

    # materialize query so that transaction should not be open while processing it
    #with_hold       = 1

    # how many records process to fetch at once and if batch processing is used then
    # also how many records are processed in one commit
    #fetch_count     = 100

    # by default commit after each row (safe when behind plproxy, bouncer or whatever)
    # can be turned off for better performance when connected directly to database
    #autocommit      = 1

    # just for tuning to throttle how much load we let onto write database
    #commit_delay    = 0.0

    # quite often data_maintainer is run from crontab and then loop delay is not needed
    # in case it has to be run as daemon set loop delay in seconds
    #loop_delay      = 1

    logfile         = ~/log/%(job_name)s.log
    pidfile         = ~/pid/%(job_name)s.pid
    use_skylog      = 0
"""

import datetime
import sys
import time

import pkgloader
pkgloader.require('skytools', '3.0')
import skytools


class DataMaintainer (skytools.DBScript):
    __doc__ = __doc__
    loop_delay = -1

    def __init__(self, args):
        super(DataMaintainer, self).__init__("data_maintainer3", args)

        # query for fetching the PK-s of the data set to be maintained
        self.sql_pk = self.cf.get("sql_get_pk_list")

        # query for changing data tuple ( autocommit )
        self.sql_modify = self.cf.get("sql_modify")

        # query to be run before starting the data maintainer,
        # useful for retrieving initialization parameters of the query
        self.sql_before = self.cf.get("sql_before_run", "")

        # query to be run after finishing the data maintainer
        self.sql_after = self.cf.get("sql_after_run", "")

        # whether to run the sql_after query in case of 0 rows
        self.after_zero_rows = self.cf.getint("after_zero_rows", 1)

        # query to be run if the process crashes
        self.sql_crash = self.cf.get("sql_on_crash", "")

        # how many records to fetch at once
        self.fetchcnt = self.cf.getint("fetchcnt", 100)
        self.fetchcnt = self.cf.getint("fetch_count", self.fetchcnt)

        # specifies if non-transactional cursor should be created (0 -> without hold)
        self.withhold = self.cf.getint("with_hold", 1)

        # execution mode (0 -> whole batch is committed / 1 -> autocommit)
        self.autocommit = self.cf.getint("autocommit", 1)

        # delay in seconds after each commit
        self.commit_delay = self.cf.getfloat("commit_delay", 0.0)

    def work(self):
        self.log.info('Starting..')
        started = lap_time = time.time()
        total_count = 0
        bres = {}

        if self.sql_before:
            bdb = self.get_database("dbbefore", autocommit=1)
            bcur = bdb.cursor()
            bcur.execute(self.sql_before)
            if bcur.statusmessage.startswith('SELECT'):
                res = bcur.fetchall()
                assert len(res)==1, "Result of a 'before' query must be 1 row"
                bres = res[0].copy()

        if self.autocommit:
            self.log.info("Autocommit after each modify")
            dbw = self.get_database("dbwrite", autocommit=1)
        else:
            self.log.info("Commit in %i record batches", self.fetchcnt)
            dbw = self.get_database("dbwrite", autocommit=0)
        if self.withhold:
            dbr = self.get_database("dbread", autocommit=1)
            sql = "DECLARE data_maint_cur NO SCROLL CURSOR WITH HOLD FOR %s"
        else:
            dbr = self.get_database("dbread", autocommit=0)
            sql = "DECLARE data_maint_cur NO SCROLL CURSOR FOR %s"
        rcur = dbr.cursor()
        mcur = dbw.cursor()
        rcur.execute(sql % self.sql_pk, bres) # pass results from before_query into sql_pk
        self.log.debug(rcur.query)
        self.log.debug(rcur.statusmessage)

        while True: # loop while fetch returns fetch_count rows
            self.fetch_started = time.time()
            rcur.execute("FETCH FORWARD %s FROM data_maint_cur" % self.fetchcnt)
            self.log.debug(rcur.query)
            self.log.debug(rcur.statusmessage)
            res = rcur.fetchall()
            count, lastitem = self.process_batch(res, mcur, bres)
            total_count += count
            if not self.autocommit:
                dbw.commit()
            self.stat_put("duration", time.time() - self.fetch_started)
            self.send_stats()
            if len(res) < self.fetchcnt:
                break
            if not self.looping:
                self.log.info("Exiting on user request")
                break
            if self.commit_delay > 0.0:
                time.sleep(self.commit_delay)
            if time.time() - lap_time > 60.0: # if one minute has passed print running totals
                self.log.info("--- Running count: %s duration: %s ---",
                        total_count, datetime.timedelta(0, round(time.time() - started)))
                lap_time = time.time()

        rcur.execute("CLOSE data_maint_cur")
        if not self.withhold:
            dbr.rollback()
        self.log.info("--- Total count: %s duration: %s ---",
                total_count, datetime.timedelta(0, round(time.time() - started)))

        if self.sql_after and (self.after_zero_rows > 0 or total_count > 0):
            adb = self.get_database("dbafter", autocommit=1)
            acur = adb.cursor()
            acur.execute(self.sql_after, lastitem)

    def process_batch(self, res, mcur, bres):
        """ Process events in autocommit mode reading results back and trying to make some sense out of them
        """
        try:
            count = 0
            item = bres.copy()
            for i in res:   # for each row in read query result
                item.update(i)
                mcur.execute(self.sql_modify, item)
                self.log.debug(mcur.query)
                if mcur.statusmessage.startswith('SELECT'): # if select was used we can expect some result
                    mres = mcur.fetchall()
                    for r in mres:
                        if 'stats' in r: # if specially handled column 'stats' is present
                            for k, v in skytools.db_urldecode(r['stats']).items():
                                self.stat_increase(k, int(v))
                        self.log.debug(r)
                else:
                    self.stat_increase('processed', mcur.rowcount)
                    self.log.debug(mcur.statusmessage)
                if 'cnt' in item:
                    count += item['cnt']
                    self.stat_increase("count", item['cnt'])
                else:
                    count += 1
                    self.stat_increase("count")
                if not self.looping:
                    break
            return count, item
        except: # process has crashed, run sql_crash and re-raise the exception
            if self.sql_crash:
                dbc = self.get_database("dbcrash", autocommit=1)
                ccur = dbc.cursor()
                ccur.execute(self.sql_crash, item)
            raise

if __name__ == '__main__':
    script = DataMaintainer(sys.argv[1:])
    script.start()
