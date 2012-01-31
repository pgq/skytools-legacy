-- File: Functions
--
--      Database functions for cascaded pgq.
--
-- Cascaded consumer flow:
--
--  - (1) [target] call pgq_node.get_consumer_state()
--  - (2) If .paused is true, sleep, go to (1).
--    This is allows to control consumer remotely.
--  - (3) If .uptodate is false, call pgq_node.set_consumer_uptodate(true).
--    This allows remote controller to know that consumer has seen the changes.
--  - (4) [source] call pgq.next_batch().  If returns NULL, sleep, goto (1)
--  - (5) [source] if batch already done, call pgq.finish_batch(), go to (1)
--  - (6) [source] read events
--  - (7) [target] process events, call pgq_node.set_consumer_completed() in same tx.
--  - (8) [source] call pgq.finish_batch()
--
-- Cascaded worker flow:
--
-- Worker is consumer that also copies to queue contents to local node (branch),
-- so it can act as provider to other nodes.  There can be only one worker per
-- node.  Or zero if node is leaf.  In addition to cascaded consumer logic above, it must -
--      - [branch] copy all events to local queue and create ticks
--      - [merge-leaf] copy all events to combined-queue
--      - [branch] publish local watermark upwards to provider so it reaches root.
--      - [branch] apply global watermark event to local node
--      - [merge-leaf] wait-behind on combined-branch (failover combined-root).
--        It's last_tick_id is set by combined-branch worker, it must call
--        pgq.next_batch()+pgq.finish_batch() without processing events
--        when behind, but not move further.  When the combined-branch
--        becomes root, it will be in right position to continue updating.
--

\i functions/pgq_node.upgrade_schema.sql
select pgq_node.upgrade_schema();

-- Group: Global Node Map
\i   functions/pgq_node.register_location.sql
\i   functions/pgq_node.unregister_location.sql
\i   functions/pgq_node.get_queue_locations.sql

-- Group: Node operations
\i   functions/pgq_node.create_node.sql
\i   functions/pgq_node.drop_node.sql
-- \i functions/pgq_node.rename_node.sql
\i   functions/pgq_node.get_node_info.sql
\i   functions/pgq_node.is_root_node.sql
\i   functions/pgq_node.is_leaf_node.sql
\i functions/pgq_node.get_subscriber_info.sql
\i functions/pgq_node.get_consumer_info.sql

\i functions/pgq_node.demote_root.sql
\i functions/pgq_node.promote_branch.sql
\i functions/pgq_node.set_node_attrs.sql

-- Group: Provider side operations - worker
\i   functions/pgq_node.register_subscriber.sql
\i   functions/pgq_node.unregister_subscriber.sql
\i   functions/pgq_node.set_subscriber_watermark.sql

-- Group: Subscriber side operations - worker
\i   functions/pgq_node.get_worker_state.sql
\i   functions/pgq_node.set_global_watermark.sql
\i   functions/pgq_node.set_partition_watermark.sql

-- Group: Subscriber side operations - any consumer
\i   functions/pgq_node.register_consumer.sql
\i   functions/pgq_node.unregister_consumer.sql
\i   functions/pgq_node.get_consumer_state.sql
\i   functions/pgq_node.change_consumer_provider.sql
\i   functions/pgq_node.set_consumer_uptodate.sql
\i   functions/pgq_node.set_consumer_paused.sql
\i   functions/pgq_node.set_consumer_completed.sql
\i   functions/pgq_node.set_consumer_error.sql

-- Group: Maintenance operations
\i functions/pgq_node.maint_watermark.sql
\i functions/pgq_node.version.sql

