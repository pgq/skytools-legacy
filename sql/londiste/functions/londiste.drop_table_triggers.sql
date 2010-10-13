
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
begin
    -- cast to bytea
    b_queue_name := replace(i_queue_name, E'\\', E'\\\\')::bytea;

    -- drop all replication triggers that target our queue.
    -- by checking trigger func and queue name there is not
    -- dependency on naming standard or side-storage.
    for logtrg_name in
        select tgname from pg_catalog.pg_trigger
         where tgrelid = londiste.find_table_oid(i_table_name)
           and tgfoid in ('pgq.sqltriga'::regproc::oid, 'pgq.logutriga'::regproc::oid)
           and substring(tgargs for (position(E'\\000'::bytea in tgargs) - 1)) = b_queue_name
    loop
        execute 'drop trigger ' || quote_ident(logtrg_name)
                || ' on ' || londiste.quote_fqname(i_table_name);
    end loop;
end;
$$ language plpgsql strict;

