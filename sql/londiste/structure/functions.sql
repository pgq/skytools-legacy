-- Section: Londiste functions

-- upgrade schema
\i functions/londiste.upgrade_schema.sql
select londiste.upgrade_schema();

-- Group: Information
\i functions/londiste.get_seq_list.sql
\i functions/londiste.get_table_list.sql
\i functions/londiste.local_show_missing.sql

-- Group: Local object registration (setup tool)
\i functions/londiste.local_add_seq.sql
\i functions/londiste.create_trigger.sql
\i functions/londiste.local_add_table.sql
\i functions/londiste.local_change_handler.sql
\i functions/londiste.local_remove_seq.sql
\i functions/londiste.local_remove_table.sql

-- Group: Global object registrations (internal)
\i functions/londiste.global_add_table.sql
\i functions/londiste.global_remove_table.sql
\i functions/londiste.global_update_seq.sql
\i functions/londiste.global_remove_seq.sql

-- Group: FKey handling
\i functions/londiste.handle_fkeys.sql

-- Group: Execute handling
\i functions/londiste.execute_start.sql
\i functions/londiste.execute_finish.sql

-- Group: Internal functions
\i functions/londiste.root_check_seqs.sql
\i functions/londiste.root_notify_change.sql
\i functions/londiste.local_set_table_state.sql
\i functions/londiste.local_set_table_attrs.sql
\i functions/londiste.local_set_table_struct.sql
\i functions/londiste.periodic_maintenance.sql

-- Group: Utility functions
\i functions/londiste.find_column_types.sql
\i functions/londiste.find_table_fkeys.sql
\i functions/londiste.find_table_oid.sql
\i functions/londiste.quote_fqname.sql
\i functions/londiste.make_fqname.sql
\i functions/londiste.split_fqname.sql
\i functions/londiste.table_info_trigger.sql
\i functions/londiste.drop_table_triggers.sql
\i functions/londiste.is_replica_func.sql
\i functions/londiste.version.sql

-- Group: Utility functions for handlers
\i functions/londiste.create_partition.sql

