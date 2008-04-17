#! /bin/sh

. ../env.sh

./stop.sh
rm -f sys/log.*

set -e


./makenode.sh test_set root root 

last=root
for n in `seq 1 10`; do
  ./makenode.sh test_set node$n branch $last
  last=node$n
done

