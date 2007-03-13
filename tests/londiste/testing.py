#! /usr/bin/env python

"""Londiste tester.
"""

import sys, os, skytools


class Tester(skytools.DBScript):
    test_pos = 0
    nr = 1
    def __init__(self, args):
        skytools.DBScript.__init__(self, 'londiste', args)
        self.log.info('start testing')

    def reload(self):
        skytools.DBScript.reload(self)
        self.loop_delay = 0.1

    def work(self):
        
        src_db = self.get_database('provider_db')
        dst_db = self.get_database('subscriber_db')
        src_curs = src_db.cursor()
        dst_curs = dst_db.cursor()
        src_curs.execute("insert into data1 (data) values ('foo%d')" % self.nr)
        src_curs.execute("insert into data2 (data) values ('foo%d')" % self.nr)
        src_db.commit()
        self.nr += 1

        if self.bad_state(dst_db, dst_curs):
            return

        if self.test_pos == 0:
            self.resync_table(dst_db, dst_curs)
            self.test_pos += 1
        elif self.test_pos == 1:
            self.run_compare()
            self.test_pos += 1

    def bad_state(self, db, curs):
        q = "select * from londiste.subscriber_table"
        curs.execute(q)
        db.commit()
        ok = 0
        bad = 0
        cnt = 0
        for row in curs.dictfetchall():
            cnt += 1
            if row['merge_state'] == 'ok':
                ok += 1
            else:
                bad += 1

        if cnt < 2:
            return 1
        if bad > 0:
            return 1

        if ok > 0:
            return 0

        return 1

    def resync_table(self, db, curs):
        self.log.info('trying to remove table')
        curs.execute("update londiste.subscriber_table"\
                     " set merge_state = null"
                     " where table_name='public.data1'")
        db.commit()

    def run_compare(self):
        args = ["londiste.py", "conf/replic.ini", "compare"]
        err = os.spawnvp(os.P_WAIT, "londiste.py", args)
        self.log.info("Compare result=%d" % err)

if __name__ == '__main__':
    script = Tester(sys.argv[1:])
    script.start()

