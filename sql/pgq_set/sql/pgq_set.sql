
\set ECHO none
\i ../txid/txid.sql
\i ../pgq/pgq.sql
\i structure/pgq_set.sql
\i structure/functions.sql
\set ECHO all

select * from pgq_set.add_member('aset', 'node1', 'dbname=node1', false);
select * from pgq_set.add_member('aset', 'node2', 'dbname=node2', false);
select * from pgq_set.add_member('aset', 'node3', 'dbname=node3', false);
select * from pgq_set.add_member('aset', 'node4', 'dbname=node4', false);
select * from pgq_set.get_member_info('aset');

select * from pgq_set.remove_member('aset', 'node4');
select * from pgq_set.get_member_info('aset');

select * from pgq_set.create_node('aset', 'root', 'node1', null, null, null);

select * from pgq_set.subscribe_node('aset', 'node2');
select * from pgq_set.subscribe_node('aset', 'node3', 1);
select * from pgq_set.unsubscribe_node('aset', 'node3');

select * from pgq_set.get_node_info('aset');

select * from pgq_set.is_root('q');
select * from pgq_set.is_root('aset');
select * from pgq_set.is_root(null);

select * from pgq_set.rename_node_step1('aset', 'node2', 'node2x');
select * from pgq_set.rename_node_step2('aset', 'node2', 'node2x');

select * from pgq_set.get_subscriber_info('aset');

