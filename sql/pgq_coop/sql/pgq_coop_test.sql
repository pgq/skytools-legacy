
\set ECHO none
\i ../pgq/pgq.sql
\i structure/schema.sql
\i structure/functions.sql
\set ECHO all

select pgq.create_queue('testqueue');
update pgq.queue set queue_ticker_max_count = 1 where queue_name = 'testqueue';

-- register
select pgq.register_consumer('testqueue', 'maincons');
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


select pgq_coop.unregister_subconsumer('testqueue', 'maincons', 'subcons1', 0);
select pgq_coop.unregister_subconsumer('testqueue', 'maincons', 'subcons1', 1);
select pgq_coop.unregister_subconsumer('testqueue', 'maincons', 'subcons1', 0);
select pgq_coop.unregister_subconsumer('testqueue', 'maincons', 'subcons2', 0);

