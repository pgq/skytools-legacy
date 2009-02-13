#! /bin/sh

. ../env.sh

pgqadm.py conf/ticker_db1.ini status
pgqadm.py conf/ticker_db2.ini status 
pgqadm.py conf/ticker_db3.ini status

setadm.py -v conf/setadm.ini status


