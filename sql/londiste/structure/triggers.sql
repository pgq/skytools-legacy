
create trigger table_info_trigger_sync before delete on londiste.table_info
for each row execute procedure londiste.table_info_trigger();

