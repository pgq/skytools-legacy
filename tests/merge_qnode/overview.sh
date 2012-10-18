#! /bin/sh

for db in part1 full1 full2; do
  echo "==== $db ==="
  psql -d $db -c "select * from pgq.get_consumer_info() where not consumer_name like '.%'"
  psql -d $db -c "select * from pgq_node.local_state order by 1,2"
done
