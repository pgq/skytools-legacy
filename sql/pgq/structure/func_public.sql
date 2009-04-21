-- Section: Public Functions

-- Group: Queue creation

\i functions/pgq.create_queue.sql
\i functions/pgq.drop_queue.sql
\i functions/pgq.set_queue_config.sql

-- Group: Event publishing

\i functions/pgq.insert_event.sql
\i functions/pgq.current_event_table.sql

-- Group: Subscribing to queue

\i functions/pgq.register_consumer.sql
\i functions/pgq.unregister_consumer.sql

-- Group: Batch processing

\i functions/pgq.next_batch.sql
\i functions/pgq.get_batch_events.sql
\i functions/pgq.get_batch_cursor.sql
\i functions/pgq.event_retry.sql
\i functions/pgq.finish_batch.sql

-- Group: General info functions

\i functions/pgq.get_queue_info.sql
\i functions/pgq.get_consumer_info.sql
\i functions/pgq.version.sql
\i functions/pgq.get_batch_info.sql

