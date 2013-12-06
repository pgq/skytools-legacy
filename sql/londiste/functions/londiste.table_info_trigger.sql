
create or replace function londiste.table_info_trigger()
returns trigger as $$
-- ----------------------------------------------------------------------
-- Function: londiste.table_info_trigger(0)
--
--      Trigger on londiste.table_info.  Cleans triggers from tables
--      when table is removed from londiste.table_info.
-- ----------------------------------------------------------------------
begin
    if TG_OP = 'DELETE' then
        perform londiste.drop_table_triggers(OLD.queue_name, OLD.table_name);
    end if;
    return OLD;
end;
$$ language plpgsql;

