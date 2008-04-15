#! /bin/sh

. ../env.sh
./testing.py -s conf/tester.ini
londiste.py -s conf/w_leaf.ini
londiste.py -s conf/w_branch.ini
londiste.py -s conf/w_root.ini

sleep 1

pgqadm.py -s conf/ticker_root.ini
pgqadm.py -s conf/ticker_branch.ini
pgqadm.py -s conf/ticker_leaf.ini

sleep 1

for f in sys/pid.*; do
  test -f "$f" || continue
  kill `cat $f`
done

