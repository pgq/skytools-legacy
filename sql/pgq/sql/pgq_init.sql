
\set ECHO none
\i ../txid/txid.sql
\i structure/install.sql

\set ECHO all

select * from pgq.maint_tables_to_vacuum();
select * from pgq.maint_retry_events();

select pgq.create_queue('tmpqueue');
select pgq.register_consumer('tmpqueue', 'consumer');
select pgq.unregister_consumer('tmpqueue', 'consumer');
select pgq.drop_queue('tmpqueue');

select pgq.create_queue('myqueue');
select pgq.register_consumer('myqueue', 'consumer');
select pgq.next_batch('myqueue', 'consumer');
select pgq.next_batch('myqueue', 'consumer');
select pgq.ticker();
select pgq.next_batch('myqueue', 'consumer');
select pgq.next_batch('myqueue', 'consumer');

select queue_name, consumer_name, prev_tick_id, tick_id, lag from pgq.get_batch_info(1);
select queue_name from pgq.get_queue_info() limit 0;
select queue_name, consumer_name from pgq.get_consumer_info() limit 0;

select pgq.finish_batch(1);
select pgq.finish_batch(1);

select pgq.ticker();
select pgq.next_batch('myqueue', 'consumer');
select * from pgq.batch_event_tables(2);
select * from pgq.get_batch_events(2);
select pgq.finish_batch(2);

select pgq.insert_event('myqueue', 'r1', 'data');
select pgq.insert_event('myqueue', 'r2', 'data');
select pgq.insert_event('myqueue', 'r3', 'data');
select pgq.current_event_table('myqueue');
select pgq.ticker();

select pgq.next_batch('myqueue', 'consumer');
select ev_id,ev_retry,ev_type,ev_data,ev_extra1,ev_extra2,ev_extra3,ev_extra4 from pgq.get_batch_events(3);

select * from pgq.failed_event_list('myqueue', 'consumer');

select pgq.event_failed(3, 1, 'failure test');
select pgq.event_failed(3, 1, 'failure test');
select pgq.event_retry(3, 2, 0);
select pgq.event_retry(3, 2, 0);
select pgq.finish_batch(3);

select ev_failed_reason, ev_id, ev_txid, ev_retry, ev_type, ev_data
  from pgq.failed_event_list('myqueue', 'consumer');

select ev_failed_reason, ev_id, ev_txid, ev_retry, ev_type, ev_data
  from pgq.failed_event_list('myqueue', 'consumer', 0, 1);

select * from pgq.failed_event_count('myqueue', 'consumer');
select * from pgq.failed_event_delete('myqueue', 'consumer', 0);

select pgq.event_retry_raw('myqueue', 'consumer', now(), 666, now(), 0,
        'rawtest', 'data', null, null, null, null);

select pgq.ticker();
