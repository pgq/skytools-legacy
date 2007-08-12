#! /bin/sh

. ./env.sh

./gendb.sh

pgqadm.py -d conf/ticker.ini ticker
queue_mover.py -d conf/mover.ini
cube_dispatcher.py -d conf/cube.ini
table_dispatcher.py -d conf/table.ini

sleep 1
psql scriptsrc <<EOF
insert into data1 (data) values ('data1.1');
insert into data1 (data) values ('data1.2');
EOF

