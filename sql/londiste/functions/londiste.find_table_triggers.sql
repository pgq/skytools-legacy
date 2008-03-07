
create or replace function londiste.find_table_triggers(i_table_name text)
returns setof londiste.pending_triggers as $$
-- ----------------------------------------------------------------------
-- Function: londiste.find_table_triggers(1)
--
--      Returns all existing triggers on table.
--
-- Parameters:
--      i_table_name - table name
--
-- Returns:
--      table_name      - fq table name
--      trigger_name    - name
--      trigger_def     - partial def as returned by pg_get_triggerdef()
-- ----------------------------------------------------------------------
declare
    tg        record;
begin
    for tg in
        select n.nspname || '.' || c.relname as table_name, t.tgname::text as name, pg_get_triggerdef(t.oid) as def 
        from pg_trigger t, pg_class c, pg_namespace n
        where n.oid = c.relnamespace and c.oid = t.tgrelid
            and t.tgrelid = londiste.find_table_oid(i_table_name)
            and not t.tgisconstraint
    loop
        return next tg;
    end loop;
    
    return;
end;
$$ language plpgsql strict stable;

