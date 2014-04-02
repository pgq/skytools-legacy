
"""Status display.
"""

import sys, skytools

__all__ = ['PGQStatus']

def ival(data, _as = None):
    "Format interval for output"
    if not _as:
        _as = data.split('.')[-1]
    numfmt = 'FM9999999'
    expr = "coalesce(to_char(extract(epoch from %s), '%s') || 's', 'NULL') as %s"
    return expr % (data, numfmt, _as)

class PGQStatus(skytools.DBScript):
    """Info gathering and display."""
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
        print("Postgres version: %s   PgQ version: %s" % (pgver, qver))

        q = """select f.queue_name, f.queue_ntables, %s, %s,
                      %s, %s, q.queue_ticker_max_count,
                      f.ev_per_sec, f.ev_new
                from pgq.get_queue_info() f, pgq.queue q
               where q.queue_name = f.queue_name""" % (
                    ival('f.queue_rotation_period'),
                    ival('f.ticker_lag'),
                    ival('q.queue_ticker_max_lag'),
                    ival('q.queue_ticker_idle_period'),
               )
        cx.execute(q)
        event_rows = cx.fetchall()

        q = """select queue_name, consumer_name, %s, %s, pending_events
               from pgq.get_consumer_info()""" % (
                ival('lag'),
                ival('last_seen'),
              )
        cx.execute(q)
        consumer_rows = cx.fetchall()

        print("\n%-33s %9s %13s %6s %6s %5s" % ('Event queue',
                            'Rotation', 'Ticker', 'TLag', 'EPS', 'New'))
        print('-' * 78)
        for ev_row in event_rows:
            tck = "%s/%s/%s" % (ev_row['queue_ticker_max_count'],
                    ev_row['queue_ticker_max_lag'],
                    ev_row['queue_ticker_idle_period'])
            rot = "%s/%s" % (ev_row['queue_ntables'], ev_row['queue_rotation_period'])
            print("%-33s %9s %13s %6s %6.1f %5d" % (
                ev_row['queue_name'],
                rot,
                tck,
                ev_row['ticker_lag'],
                ev_row['ev_per_sec'],
                ev_row['ev_new'],
            ))
        print('-' * 78)
        print("\n%-48s %9s %9s %8s" % (
                'Consumer', 'Lag', 'LastSeen', 'Pending'))
        print('-' * 78)
        for ev_row in event_rows:
            cons = self.pick_consumers(ev_row, consumer_rows)
            self.show_queue(ev_row, cons)
        print('-' * 78)
        db.commit()

    def show_consumer(self, cons):
        print("  %-46s %9s %9s %8d" % (
                    cons['consumer_name'],
                    cons['lag'], cons['last_seen'],
                    cons['pending_events']))

    def show_queue(self, ev_row, consumer_rows):
        print("%(queue_name)s:" % ev_row)
        for cons in consumer_rows:
            self.show_consumer(cons)


    def pick_consumers(self, ev_row, consumer_rows):
        res = []
        for con in consumer_rows:
            if con['queue_name'] != ev_row['queue_name']:
                continue
            res.append(con)
        return res

