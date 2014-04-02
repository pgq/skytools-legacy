#!/usr/bin/env python

"""Generic script for processing large data sets in small batches.

Reads events from one datasource and commits them into another one,
either one by one or in batches.

Config template::

    [data_maintainer3]
    job_name        = dm_remove_expired_services

    # if source is database, you need to specify dbread and sql_get_pk_list
    dbread          = dbname=sourcedb_test
    sql_get_pk_list =
        select username
        from user_service
        where expire_date < now();

    # if source is csv file, you need to specify fileread and optionally csv_delimiter and csv_quotechar
    #fileread       = data.csv
    #csv_delimiter  = ,
    #csv_quotechar  = "

    dbwrite         = dbname=destdb port=1234 host=dbhost.com user=guest password=secret
    dbbefore        = dbname=destdb_test
    dbafter         = dbname=destdb_test
    dbcrash         = dbname=destdb_test
    dbthrottle      = dbname=queuedb_test

    # It is a good practice to include same where condition on target side as on read side,
    # to ensure that you are actually changing the same data you think you are,
    # especially when reading from replica database or when processing takes days.
    sql_modify =
        delete from user_service
        where username = %%(username)s
        and expire_date < now();

    # This will be run before executing the sql_get_pk_list query (optional)
    #sql_before_run =
    #    select * from somefunction1(%(job_name)s);

    # This will be run when the DM finishes (optional)
    #sql_after_run =
    #    select * from somefunction2(%(job_name)s);

    # Determines whether the sql_after_run query will be run in case the pk list query returns no rows
    #after_zero_rows = 1

    # This will be run if the DM crashes (optional)
    #sql_on_crash =
    #    select * from somefunction3(%(job_name)s);

    # This may be used to control throttling of the DM (optional)
    #sql_throttle =
    #    select lag>'5 minutes'::interval from pgq.get_consumer_info('failoverconsumer');

    # materialize query so that transaction should not be open while processing it (only used when source is a database)
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

import csv
import datetime
import os.path
import sys
import time

import pkgloader
pkgloader.require('skytools', '3.0')
import skytools


class DataSource (object):
    def __init__(self, log):
        self.log = log

    def open(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def fetch(self, count):
        raise NotImplementedError


class DBDataSource (DataSource):
    def __init__(self, log, db, query, bres = None, with_hold = False):
        super(DBDataSource, self).__init__(log)
        self.db = db
        if with_hold:
            self.query = "DECLARE data_maint_cur NO SCROLL CURSOR WITH HOLD FOR %s" % query
        else:
            self.query = "DECLARE data_maint_cur NO SCROLL CURSOR FOR %s" % query
        self.bres = bres
        self.with_hold = with_hold

    def _run_query(self, query, params = None):
        self.cur.execute(query, params)
        self.log.debug(self.cur.query)
        self.log.debug(self.cur.statusmessage)

    def open(self):
        self.cur = self.db.cursor()
        self._run_query(self.query, self.bres)  # pass results from before_query into sql_pk

    def close(self):
        self.cur.execute("CLOSE data_maint_cur")
        if not self.with_hold:
            self.db.rollback()

    def fetch(self, count):
        self._run_query("FETCH FORWARD %i FROM data_maint_cur" % count)
        return self.cur.fetchall()


class CSVDataSource (DataSource):
    def __init__(self, log, filename, delimiter, quotechar):
        super(CSVDataSource, self).__init__(log)
        self.filename = filename
        self.delimiter = delimiter
        self.quotechar = quotechar

    def open(self):
        self.fp = open(self.filename, 'rb')
        self.reader = csv.DictReader(self.fp, delimiter = self.delimiter, quotechar = self.quotechar)

    def close(self):
        self.fp.close()

    def fetch(self, count):
        ret = []
        for row in self.reader:
            ret.append(row)
            count -= 1
            if count <= 0:
                break
        return ret


class DataMaintainer (skytools.DBScript):
    __doc__ = __doc__
    loop_delay = -1

    def __init__(self, args):
        super(DataMaintainer, self).__init__("data_maintainer3", args)

        # source file
        self.fileread = self.cf.get("fileread", "")
        if self.fileread:
            self.fileread = os.path.expanduser(self.fileread)
            self.set_single_loop(True)  # force single run if source is file

        self.csv_delimiter = self.cf.get("csv_delimiter", ',')
        self.csv_quotechar = self.cf.get("csv_quotechar", '"')

        # query for fetching the PK-s of the data set to be maintained
        self.sql_pk = self.cf.get("sql_get_pk_list", "")

        if (int(bool(self.sql_pk)) + int(bool(self.fileread))) in (0,2):
            raise skytools.UsageError("Either fileread or sql_get_pk_list must be specified in the configuration file")

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

        # query for checking if / how much to throttle
        self.sql_throttle = self.cf.get("sql_throttle", "")

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
        self.started = self.lap_time = time.time()
        self.total_count = 0
        bres = {}

        if self.sql_before:
            bdb = self.get_database("dbbefore", autocommit=1)
            bcur = bdb.cursor()
            bcur.execute(self.sql_before)
            if bcur.statusmessage.startswith('SELECT'):
                res = bcur.fetchall()
                assert len(res)==1, "Result of a 'before' query must be 1 row"
                bres = res[0].copy()

        if self.sql_throttle:
            dbt = self.get_database("dbthrottle", autocommit=1)
            tcur = dbt.cursor()

        if self.autocommit:
            self.log.info("Autocommit after each modify")
            dbw = self.get_database("dbwrite", autocommit=1)
        else:
            self.log.info("Commit in %i record batches", self.fetchcnt)
            dbw = self.get_database("dbwrite", autocommit=0)

        if self.fileread:
            self.datasource = CSVDataSource(self.log, self.fileread, self.csv_delimiter, self.csv_quotechar)
        else:
            if self.withhold:
                dbr = self.get_database("dbread", autocommit=1)
            else:
                dbr = self.get_database("dbread", autocommit=0)
            self.datasource = DBDataSource(self.log, dbr, self.sql_pk, bres, self.withhold)

        self.datasource.open()
        mcur = dbw.cursor()

        while True: # loop while fetch returns fetch_count rows
            self.fetch_started = time.time()
            res = self.datasource.fetch(self.fetchcnt)
            count, lastitem = self.process_batch(res, mcur, bres)
            self.total_count += count
            if not self.autocommit:
                dbw.commit()
            self.stat_put("duration", time.time() - self.fetch_started)
            self.send_stats()
            if len(res) < self.fetchcnt or self.last_sigint:
                break
            if self.commit_delay > 0.0:
                time.sleep(self.commit_delay)
            if self.sql_throttle:
                self.throttle(tcur)
            self._print_count("--- Running count: %s duration: %s ---")

        if self.last_sigint:
            self.log.info("Exiting on user request")

        self.datasource.close()
        self.log.info("--- Total count: %s duration: %s ---",
                self.total_count, datetime.timedelta(0, round(time.time() - self.started)))

        if self.sql_after and (self.after_zero_rows > 0 or self.total_count > 0):
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
                            for k, v in skytools.db_urldecode(r['stats'] or '').items():
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
                if self.last_sigint:
                    break
            return count, item
        except: # process has crashed, run sql_crash and re-raise the exception
            if self.sql_crash:
                dbc = self.get_database("dbcrash", autocommit=1)
                ccur = dbc.cursor()
                ccur.execute(self.sql_crash, item)
            raise

    def throttle(self, tcur):
        while not self.last_sigint:
            tcur.execute(self.sql_throttle)
            _r = tcur.fetchall()
            assert len(_r) == 1 and len(_r[0]) == 1, "Result of 'throttle' query must be 1 value"
            throttle = _r[0][0]
            if isinstance(throttle, bool):
                tt = float(throttle and 30)
            elif isinstance(throttle, (int, float)):
                tt = float(throttle)
            else:
                self.log.warn("Result of 'throttle' query must be boolean or numeric")
                break
            if tt > 0.0:
                self.log.debug("sleeping %f s", tt)
                time.sleep(tt)
            else:
                break
            self._print_count("--- Waiting count: %s duration: %s ---")

    def _print_count(self, text):
        if time.time() - self.lap_time > 60.0: # if one minute has passed print running totals
            self.log.info(text, self.total_count, datetime.timedelta(0, round(time.time() - self.started)))
            self.lap_time = time.time()


if __name__ == '__main__':
    script = DataMaintainer(sys.argv[1:])
    script.start()
