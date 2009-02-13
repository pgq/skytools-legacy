#! /bin/sh

. ../env.sh

./plainconsumer.py -v conf/nop_consumer.ini --register
./plainconsumer.py -v conf/nop_consumer.ini

