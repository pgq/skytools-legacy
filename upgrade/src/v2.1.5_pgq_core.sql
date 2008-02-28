begin;

alter table pgq.subscription add constraint subscription_ukey unique (sub_queue, sub_consumer);
create index rq_retry_owner_idx on pgq.retry_queue (ev_owner, ev_id);

\i ../sql/pgq/functions/pgq.current_event_table.sql
\i ../sql/pgq/functions/pgq.event_failed.sql
\i ../sql/pgq/functions/pgq.event_retry.sql
\i ../sql/pgq/functions/pgq.force_tick.sql
\i ../sql/pgq/functions/pgq.grant_perms.sql
\i ../sql/pgq/functions/pgq.insert_event.sql
\i ../sql/pgq/functions/pgq.maint_tables_to_vacuum.sql
\i ../sql/pgq/functions/pgq.next_batch.sql
\i ../sql/pgq/functions/pgq.register_consumer.sql
\i ../sql/pgq/functions/pgq.version.sql
\i ../sql/pgq/structure/grants.sql

end;

