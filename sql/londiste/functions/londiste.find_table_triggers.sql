
create or replace function londiste.find_table_triggers(i_table_name text)
returns setof londiste.subscriber_pending_triggers as $$
declare
    tg        record;
    ver       int4;
begin
    select setting::int4 into ver from pg_settings
     where name = 'server_version_num';

    if ver >= 90000 then
        for tg in
            select n.nspname || '.' || c.relname as table_name, t.tgname::text as name, pg_get_triggerdef(t.oid) as def 
            from pg_trigger t, pg_class c, pg_namespace n
            where n.oid = c.relnamespace and c.oid = t.tgrelid
                and t.tgrelid = londiste.find_table_oid(i_table_name)
                and not t.tgisinternal
        loop
            return next tg;
        end loop;
    else
        for tg in
            select n.nspname || '.' || c.relname as table_name, t.tgname::text as name, pg_get_triggerdef(t.oid) as def 
            from pg_trigger t, pg_class c, pg_namespace n
            where n.oid = c.relnamespace and c.oid = t.tgrelid
                and t.tgrelid = londiste.find_table_oid(i_table_name)
                and not t.tgisconstraint
        loop
            return next tg;
        end loop;
    end if;
    
    return;
end;
$$ language plpgsql strict stable;
