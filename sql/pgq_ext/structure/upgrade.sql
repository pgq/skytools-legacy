--
-- Section: Functions
--

\i functions/pgq_ext.upgrade_schema.sql

select pgq_ext.upgrade_schema();

-- Group: track batches via batch id
\i functions/pgq_ext.is_batch_done.sql
\i functions/pgq_ext.set_batch_done.sql

-- Group: track batches via tick id
\i functions/pgq_ext.get_last_tick.sql
\i functions/pgq_ext.set_last_tick.sql


-- Group: Track events separately
\i functions/pgq_ext.is_event_done.sql
\i functions/pgq_ext.set_event_done.sql

-- Group: Schema info
\i functions/pgq_ext.version.sql

