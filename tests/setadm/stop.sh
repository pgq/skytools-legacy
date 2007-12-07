#! /bin/sh

. ../env.sh

./testconsumer.py -s conf/zroot.ini
./testconsumer.py -s conf/zbranch.ini
./testconsumer.py -s conf/zleaf.ini

#sleep 1

#pgqadm.py -s conf/ticker.ini
#pgqadm.py -s conf/linkticker.ini

