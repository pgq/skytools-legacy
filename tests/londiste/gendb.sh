#! /bin/sh

. ../env.sh

mkdir -p sys
./stop.sh
rm -f sys/*.log sys/*.ini sys/*.log.[0-9]

set -e


./makenode.sh test_set root root 

./makenode.sh test_set node1 branch root
londiste.py sys/worker_root.ini status

londiste.py sys/worker_root.ini rename-node n_node1 node1renamed

#exit 0

./makenode.sh test_set node2 branch root
./makenode.sh test_set node3 branch root

./makenode.sh test_set node4 branch node1
./makenode.sh test_set node5 branch node1
./makenode.sh test_set node6 branch node1

./makenode.sh test_set node7 branch node5

./makenode.sh test_set node8 branch node2

./makenode.sh test_set node9 branch node3
./makenode.sh test_set node10 branch node3
./makenode.sh test_set node11 branch node3
#./makenode.sh test_set node12 branch node3
#./makenode.sh test_set node13 branch node3

londiste.py sys/worker_root.ini status

#last=root
#for n in `seq 1 10`; do
#  ./makenode.sh test_set node$n branch $last
#  last=node$n
#done

