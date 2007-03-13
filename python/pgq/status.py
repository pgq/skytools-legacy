
"""Status display.
"""

import sys, os, skytools

def ival(data, as = None):
    "Format interval for output"
    if not as:
        as = data.split('.')[-1]
    numfmt = 'FM9999999'
    expr = "coalesce(to_char(extract(epoch from %s), '%s') || 's', 'NULL') as %s"
    return expr % (data, numfmt, as)

class PGQStatus(skytools.DBScript):
    def __init__(self, args, check = 0):
        skytools.DBScript.__init__(self, 'pgqadm', args)

        self.show_status()

        sys.exit(0)

    def show_status(self):
        db = self.get_database("db", autocommit=1)
        cx = db.cursor()

        cx.execute("show server_version")
        pgver = cx.fetchone()[0]
        cx.execute("select pgq.version()")
        qver = cx.fetchone()[0]
        print "Postgres version: %s   PgQ version: %s" % (pgver, qver)

        q = """select f.queue_name, f.num_tables, %s, %s, %s,
                      q.queue_ticker_max_lag, q.queue_ticker_max_amount,
                      q.queue_ticker_idle_interval
               from pgq.get_queue_info() f, pgq.queue q
               where q.queue_name = f.queue_name""" % (
                    ival('f.rotation_delay'),
                    ival('f.ticker_lag'),
               )
        cx.execute(q)
        event_rows = cx.dictfetchall()

        q = """select queue_name, consumer_name, %s, %s, %s
               from pgq.get_consumer_info()""" % (
                ival('lag'),
                ival('last_seen'),
              )
        cx.execute(q)
        consumer_rows = cx.dictfetchall()

        print "\n%-32s %s %9s %13s %6s" % ('Event queue',
                            'Rotation', 'Ticker', 'TLag')
        print '-' * 78
        for ev_row in event_rows:
            tck = "%s/%ss/%ss" % (ev_row['queue_ticker_max_amount'],
                    ev_row['queue_ticker_max_lag'],
                    ev_row['queue_ticker_idle_interval'])
            rot = "%s/%s" % (ev_row['queue_ntables'], ev_row['queue_rotation_period'])
            print   "%-39s%7s %9s %13s %6s" % (
                ev_row['queue_name'],
                rot,
                tck,
                ev_row['ticker_lag'],
            )
        print '-' * 78
        print "\n%-42s %9s %9s" % (
                'Consumer', 'Lag', 'LastSeen')
        print '-' * 78
        for ev_row in event_rows:
            cons = self.pick_consumers(ev_row, consumer_rows)
            self.show_queue(ev_row, cons)
        print '-' * 78
        db.commit()

    def show_consumer(self, cons):
        print "  %-48s %9s %9s" % (
                    cons['consumer_name'],
                    cons['lag'], cons['last_seen'])
    def show_queue(self, ev_row, consumer_rows):
        print "%(queue_name)s:" % ev_row
        for cons in consumer_rows:
            self.show_consumer(cons)


    def pick_consumers(self, ev_row, consumer_rows):
        res = []
        for con in consumer_rows:
            if con['queue_name'] != ev_row['queue_name']:
                continue
            res.append(con)
        return res

