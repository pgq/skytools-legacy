#! /bin/sh

. ../env.sh

pgqadm conf/ticker_db1.ini status
pgqadm conf/ticker_db2.ini status 
pgqadm conf/ticker_db3.ini status

setadm -v conf/setadm.ini status


