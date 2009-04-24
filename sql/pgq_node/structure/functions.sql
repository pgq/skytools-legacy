-- File: Functions
--
--      Database functions for cascaded pgq.

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
\i functions/pgq_node.get_subscriber_info.sql
\i functions/pgq_node.get_consumer_info.sql

\i functions/pgq_node.demote_root.sql
\i functions/pgq_node.promote_branch.sql

-- Group: Provider side operations - worker
\i   functions/pgq_node.register_subscriber.sql
\i   functions/pgq_node.unregister_subscriber.sql
\i   functions/pgq_node.set_subscriber_watermark.sql

-- Group: Subscriber side operations - worker
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

