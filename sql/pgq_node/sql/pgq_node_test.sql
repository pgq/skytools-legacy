
select * from pgq_node.register_location('aqueue', 'node1', 'dbname=node1', false);
select * from pgq_node.register_location('aqueue', 'node2', 'dbname=node2', false);
select * from pgq_node.register_location('aqueue', 'node3', 'dbname=node3', false);
select * from pgq_node.register_location('aqueue', 'node4', 'dbname=node44', false);
select * from pgq_node.register_location('aqueue', 'node4', 'dbname=node4', false);
select * from pgq_node.register_location('aqueue', 'node5', 'dbname=node4', false);
select * from pgq_node.get_queue_locations('aqueue');
select * from pgq_node.unregister_location('aqueue', 'node5');
select * from pgq_node.unregister_location('aqueue', 'node5');
select * from pgq_node.get_queue_locations('aqueue');

select * from pgq_node.create_node('aqueue', 'root', 'node1', 'node1_worker', null, null, null);
select * from pgq_node.register_subscriber('aqueue', 'node2', 'node2_worker', null);
select * from pgq_node.register_subscriber('aqueue', 'node3', 'node3_worker', null);

select * from pgq_node.maint_watermark('aqueue');
select * from pgq_node.maint_watermark('aqueue-x');

select * from pgq_node.get_consumer_info('aqueue');
select * from pgq_node.unregister_subscriber('aqueue', 'node3');
select queue_name, consumer_name, last_tick from pgq.get_consumer_info();

select * from pgq_node.get_worker_state('aqueue');

update pgq.queue set queue_ticker_max_lag = '0', queue_ticker_idle_period = '0';
select * from pgq.ticker('aqueue');
select * from pgq.ticker('aqueue');
select * from pgq_node.set_subscriber_watermark('aqueue', 'node2', 3);
select queue_name, consumer_name, last_tick from pgq.get_consumer_info();

select * from pgq_node.set_node_attrs('aqueue', 'test=1');

select * from pgq_node.get_node_info('aqueue');
select * from pgq_node.get_subscriber_info('aqueue');

-- branch node
select * from pgq_node.register_location('bqueue', 'node1', 'dbname=node1', false);
select * from pgq_node.register_location('bqueue', 'node2', 'dbname=node2', false);
select * from pgq_node.register_location('bqueue', 'node3', 'dbname=node3', false);
select * from pgq_node.create_node('bqueue', 'branch', 'node2', 'node2_worker', 'node1', 1, null);

select * from pgq_node.register_consumer('bqueue', 'random_consumer', 'node1', 1);
select * from pgq_node.register_consumer('bqueue', 'random_consumer2', 'node1', 1);

select * from pgq_node.local_state;
select * from pgq_node.node_info;

select * from pgq_node.get_node_info('aqueue');
select * from pgq_node.get_node_info('bqueue');
select * from pgq_node.get_node_info('cqueue');

select * from pgq_node.get_worker_state('aqueue');
select * from pgq_node.get_worker_state('bqueue');
select * from pgq_node.get_worker_state('cqueue');

select * from pgq_node.is_root_node('aqueue');
select * from pgq_node.is_root_node('bqueue');
select * from pgq_node.is_root_node('cqueue');

select * from pgq_node.get_consumer_state('bqueue', 'random_consumer');
select * from pgq_node.get_consumer_state('bqueue', 'random_consumer2');

select * from pgq_node.set_consumer_error('bqueue', 'random_consumer2', 'failure');
select * from pgq_node.get_consumer_state('bqueue', 'random_consumer2');

select * from pgq_node.set_consumer_completed('bqueue', 'random_consumer2', 2);
select * from pgq_node.get_consumer_state('bqueue', 'random_consumer2');

select * from pgq_node.set_consumer_paused('bqueue', 'random_consumer2', true);
select * from pgq_node.set_consumer_uptodate('bqueue', 'random_consumer2', true);

select * from pgq_node.change_consumer_provider('bqueue', 'random_consumer2', 'node3');
select * from pgq_node.get_consumer_state('bqueue', 'random_consumer2');

select * from pgq_node.unregister_consumer('bqueue', 'random_consumer2');
select * from pgq_node.get_consumer_state('bqueue', 'random_consumer2');

select * from pgq_node.get_node_info('bqueue');

set session_replication_role = 'replica';

select * from pgq_node.demote_root('aqueue', 1, 'node3');
select * from pgq_node.demote_root('aqueue', 1, 'node3');
select * from pgq_node.demote_root('aqueue', 2, 'node3');
select * from pgq_node.demote_root('aqueue', 2, 'node3');
select * from pgq_node.demote_root('aqueue', 3, 'node3');
select * from pgq_node.demote_root('aqueue', 3, 'node3');

-- leaf node
select * from pgq_node.register_location('mqueue', 'node1', 'dbname=node1', false);
select * from pgq_node.register_location('mqueue', 'node2', 'dbname=node2', false);
select * from pgq_node.register_location('mqueue', 'node3', 'dbname=node3', false);
select * from pgq_node.create_node('mqueue', 'leaf', 'node2', 'node2_worker', 'node1', 13, 'aqueue');
select * from pgq_node.get_worker_state('mqueue');

select * from pgq_node.drop_node('asd', 'asd');
select * from pgq_node.drop_node('mqueue', 'node3');
select * from pgq_node.drop_node('mqueue', 'node2');
select * from pgq_node.drop_node('mqueue', 'node1');
select * from pgq_node.drop_node('aqueue', 'node5');
select * from pgq_node.drop_node('aqueue', 'node4');
select * from pgq_node.drop_node('aqueue', 'node1');
select * from pgq_node.drop_node('aqueue', 'node2');
select * from pgq_node.drop_node('aqueue', 'node3');

\q

select * from pgq_node.subscribe_node('aqueue', 'node2');
select * from pgq_node.subscribe_node('aqueue', 'node3', 1);
select * from pgq_node.unsubscribe_node('aqueue', 'node3');

select * from pgq_node.get_node_info('aqueue');

select * from pgq_node.is_root('q');
select * from pgq_node.is_root('aqueue');
select * from pgq_node.is_root(null);

select * from pgq_node.rename_node_step1('aqueue', 'node2', 'node2x');
select * from pgq_node.rename_node_step2('aqueue', 'node2', 'node2x');

select * from pgq_node.get_subscriber_info('aqueue');


