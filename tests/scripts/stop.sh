#! /bin/sh

. ./env.sh

cube_dispatcher.py -s conf/cube.ini
table_dispatcher.py -s conf/table.ini
queue_mover.py -s conf/mover.ini

sleep 1

pgqadm.py -s conf/ticker.ini

#killall python

