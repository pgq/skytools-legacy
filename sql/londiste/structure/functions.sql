-- Section: Londiste functions

-- Group: Main operations
\i functions/londiste.node_add_seq.sql
\i functions/londiste.node_add_table.sql
\i functions/londiste.node_get_seq_list.sql
\i functions/londiste.node_get_table_list.sql
\i functions/londiste.node_remove_seq.sql
\i functions/londiste.node_remove_table.sql
\i functions/londiste.node_set_table_state.sql

-- Group: Set object registrations
\i functions/londiste.set_add_table.sql
\i functions/londiste.set_remove_table.sql
\i functions/londiste.set_get_table_list.sql

-- Group: FKey handling
\i functions/londiste.handle_fkeys.sql

-- Group: Trigger handling
\i functions/londiste.handle_triggers.sql

-- Group: Internal functions
\i functions/londiste.node_set_skip_truncate.sql
\i functions/londiste.node_prepare_triggers.sql
\i functions/londiste.node_refresh_triggers.sql
\i functions/londiste.node_disable_triggers.sql
\i functions/londiste.root_notify_change.sql

-- Group: Utility functions
\i functions/londiste.find_column_types.sql
\i functions/londiste.find_table_fkeys.sql
\i functions/londiste.find_table_oid.sql
\i functions/londiste.find_table_triggers.sql
\i functions/londiste.quote_fqname.sql
\i functions/londiste.make_fqname.sql

