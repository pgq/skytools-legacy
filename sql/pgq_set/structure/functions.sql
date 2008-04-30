
-- Group: Global Node Map
\i functions/pgq_set.add_member.sql
\i functions/pgq_set.remove_member.sql
\i functions/pgq_set.get_member_info.sql

-- Group: Node manipulation
\i functions/pgq_set.create_node.sql
\i functions/pgq_set.drop_node.sql
\i functions/pgq_set.subscribe_node.sql
\i functions/pgq_set.unsubscribe_node.sql
\i functions/pgq_set.set_node_uptodate.sql
\i functions/pgq_set.set_node_paused.sql
\i functions/pgq_set.change_provider.sql
\i functions/pgq_set.drop_member.sql
\i functions/pgq_set.rename_node.sql

-- Group: Node Info
\i functions/pgq_set.get_node_info.sql
\i functions/pgq_set.get_subscriber_info.sql
\i functions/pgq_set.is_root.sql

-- Group: Watermark tracking
\i functions/pgq_set.set_subscriber_watermark.sql
\i functions/pgq_set.set_global_watermark.sql
\i functions/pgq_set.set_partition_watermark.sql

\i functions/pgq_set.track_tick.sql

