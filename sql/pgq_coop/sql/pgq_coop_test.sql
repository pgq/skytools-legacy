
select pgq.create_queue('testqueue');
update pgq.queue set queue_ticker_max_count = 1 where queue_name = 'testqueue';

-- register
select pgq_coop.register_subconsumer('testqueue', 'maincons', 'subcons1');
select pgq_coop.register_subconsumer('testqueue', 'maincons', 'subcons1');
select pgq_coop.register_subconsumer('testqueue', 'maincons', 'subcons1');
select pgq_coop.register_subconsumer('testqueue', 'maincons', 'subcons2');

-- process events
select pgq_coop.next_batch('testqueue', 'maincons', 'subcons1');
select pgq.insert_event('testqueue', 'ev0', 'data');
select pgq.insert_event('testqueue', 'ev1', 'data');
select pgq.insert_event('testqueue', 'ev2', 'data');
select pgq.ticker();

select pgq_coop.next_batch('testqueue', 'maincons', 'subcons1');
select pgq_coop.next_batch('testqueue', 'maincons', 'subcons1');

select pgq_coop.next_batch('testqueue', 'maincons', 'subcons2');

select pgq.insert_event('testqueue', 'ev3', 'data');
select pgq.insert_event('testqueue', 'ev4', 'data');
select pgq.insert_event('testqueue', 'ev5', 'data');
select pgq.ticker();
select pgq_coop.next_batch('testqueue', 'maincons', 'subcons2');

select pgq_coop.finish_batch(2);

-- test takeover
select pgq_coop.next_batch('testqueue', 'maincons', 'subcons2', '1 hour');
update pgq.subscription set sub_active = '2005-01-01' where sub_batch is not null;
select pgq_coop.next_batch('testqueue', 'maincons', 'subcons2', '1 hour');

select pgq_coop.unregister_subconsumer('testqueue', 'maincons', 'subcons1', 0);
select pgq_coop.unregister_subconsumer('testqueue', 'maincons', 'subcons2', 0);
select pgq_coop.unregister_subconsumer('testqueue', 'maincons', 'subcons2', 1);
select pgq_coop.unregister_subconsumer('testqueue', 'maincons', 'subcons2', 0);

-- test auto-creation
select pgq_coop.next_batch('testqueue', 'cmain', 'sc1');
select pgq_coop.next_batch('testqueue', 'cmain', 'sc2');
select consumer_name, last_tick from pgq.get_consumer_info();

-- test unregistering with pure pgq api
select pgq.unregister_consumer('testqueue', 'cmain.sc2');
select pgq.unregister_consumer('testqueue', 'cmain');
select consumer_name, last_tick from pgq.get_consumer_info();

