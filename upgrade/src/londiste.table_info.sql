ALTER TABLE londiste.table_info DROP CONSTRAINT table_info_check;
ALTER TABLE londiste.table_info ADD CHECK (dropped_ddl is null or merge_state in ('in-copy', 'catching-up'));
