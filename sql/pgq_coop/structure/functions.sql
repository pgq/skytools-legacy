
-- ----------------------------------------------------------------------
-- Section: Functions
--
-- Overview:
-- 
-- The usual flow of a cooperative consumer is to
-- 
--  1. register itself as a subconsumer for a queue:
--      pgq_coop.register_subconsumer() 
-- 
-- And the run a loop doing
--
--  2A. pgq_coop.next_batch ()
--
--  2B. pgq_coop.finish_batch()
-- 
-- Once the cooperative (or sub-)consuber is done, it should unregister 
-- itself before exiting
-- 
--  3. pgq_coop.unregister_subconsumer() 
-- 
-- 
-- ----------------------------------------------------------------------

-- Group: Subconsumer registration
\i functions/pgq_coop.register_subconsumer.sql
\i functions/pgq_coop.unregister_subconsumer.sql

-- Group: Event processing
\i functions/pgq_coop.next_batch.sql
\i functions/pgq_coop.finish_batch.sql

-- Group: General Info
\i functions/pgq_coop.version.sql

