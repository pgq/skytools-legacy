#! /bin/sh

. ../env.sh
./testing.py -s conf/tester.ini
londiste.py -s conf/fwrite.ini
londiste.py -s conf/replic.ini

sleep 1

pgqadm.py -s conf/ticker.ini
pgqadm.py -s conf/linkticker.ini

