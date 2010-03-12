begin;

-- new fields to pgq.queue
alter table pgq.queue add column queue_disable_insert boolean;
alter table pgq.queue add column queue_ticker_paused boolean;
alter table pgq.queue add column queue_per_tx_limit int4;
update pgq.queue set queue_disable_insert=false, queue_ticker_paused=false;
alter table pgq.queue alter column queue_disable_insert set not null;
alter table pgq.queue alter column queue_disable_insert set default false;
alter table pgq.queue alter column queue_ticker_paused set not null;
alter table pgq.queue alter column queue_ticker_paused set default false;

-- new field to pgq.tick
alter table pgq.tick add column tick_event_seq int8;

-- surgery on pgq.retry_queue
alter table pgq.retry_queue add column ev_queue int4;
update pgq.retry_queue set ev_queue = sub_queue
  from pgq.subscription where ev_owner = sub_id;
alter table pgq.retry_queue alter column ev_queue set not null;
drop index pgq.rq_retry_owner_idx;

-- surgery on pgq.subscription
alter table pgq.retry_queue drop constraint rq_owner_fkey;
alter table pgq.failed_queue drop constraint fq_owner_fkey;
alter table pgq.subscription drop constraint subscription_pkey;
alter table pgq.subscription drop constraint subscription_ukey;
alter table pgq.subscription add constraint subscription_pkey primary key (sub_queue, sub_consumer);
alter table pgq.subscription add constraint subscription_batch_idx unique (sub_batch);
alter table pgq.subscription alter column sub_last_tick drop not null;

-- drop failed queue functionality.  not mandatory, who wants can keep it.
drop function pgq.failed_event_list(text, text);
drop function pgq.failed_event_list(text, text, integer, integer);
drop function pgq.failed_event_count(text, text);
drop function pgq.failed_event_delete(text, text, bigint);
drop function pgq.failed_event_retry(text, text, bigint);
drop function pgq.event_failed(bigint, bigint, text);
drop table pgq.failed_queue;

-- drop obsolete functions
drop function pgq.ticker(text, bigint);
drop function pgq.register_consumer(text, text, int8);

-- drop types and related functions
drop function pgq.get_batch_events(bigint);
drop function pgq.get_batch_info(bigint);
drop function pgq.get_consumer_info();
drop function pgq.get_consumer_info(text);
drop function pgq.get_consumer_info(text, text);
drop function pgq.get_queue_info();
drop function pgq.get_queue_info(text);
drop type pgq.ret_batch_event;
drop type pgq.ret_batch_info;
drop type pgq.ret_consumer_info;
drop type pgq.ret_queue_info;

-- update all functions
\i ../sql/pgq/pgq.upgrade.sql

end;

