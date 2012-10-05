
create or replace function londiste.drop_table_triggers(
    in i_queue_name text, in i_table_name text)
returns void as $$
-- ----------------------------------------------------------------------
-- Function: londiste.drop_table_triggers(2)
--
--      Remove Londiste triggers from table.
--
-- Parameters:
--      i_queue_name      - set name
--      i_table_name      - table name
--
-- Returns:
--      200 - OK
--      404 - Table not found
-- ----------------------------------------------------------------------
declare
    logtrg_name     text;
    b_queue_name    bytea;
    _dest_table     text;
begin
    select coalesce(dest_table, table_name)
        from londiste.table_info t
        where t.queue_name = i_queue_name
          and t.table_name = i_table_name
        into _dest_table;
    if not found then
        return;
    end if;

    -- skip if no triggers found on that table
    perform 1 from pg_catalog.pg_trigger where tgrelid = londiste.find_table_oid(_dest_table);
    if not found then
        return;
    end if;

    -- cast to bytea
    b_queue_name := decode(replace(i_queue_name, E'\\', E'\\\\'), 'escape');

    -- drop all replication triggers that target our queue.
    -- by checking trigger func and queue name there is not
    -- dependency on naming standard or side-storage.
    for logtrg_name in
        select tgname from pg_catalog.pg_trigger
         where tgrelid = londiste.find_table_oid(_dest_table)
           and londiste.is_replica_func(tgfoid)
           and octet_length(tgargs) > 0
           and substring(tgargs for (position(E'\\000'::bytea in tgargs) - 1)) = b_queue_name
    loop
        execute 'drop trigger ' || quote_ident(logtrg_name)
                || ' on ' || londiste.quote_fqname(_dest_table);
    end loop;
end;
$$ language plpgsql strict;

