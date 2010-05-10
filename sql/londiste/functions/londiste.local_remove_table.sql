
create or replace function londiste.local_remove_table(
    in i_queue_name text, in i_table_name text,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_remove_table(2)
--
--      Remove table.
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
    fq_table_name   text;
    logtrg_name     text;
    tbl             record;
    b_queue_name    bytea;
begin
    fq_table_name := londiste.make_fqname(i_table_name);

    select local into tbl
        from londiste.table_info
        where queue_name = i_queue_name
          and table_name = fq_table_name;
    if not found then
        select 400, 'Table not found: ' || fq_table_name into ret_code, ret_note;
        return;
    end if;

    if tbl.local then
        -- cast to bytea
        b_queue_name := replace(i_queue_name, E'\\', E'\\\\')::bytea;

        -- drop all replication triggers that target our queue.
        -- by checking trigger func and queue name there is not
        -- dependency on naming standard or side-storage.
        for logtrg_name in
            select tgname from pg_catalog.pg_trigger
             where tgrelid = londiste.find_table_oid(fq_table_name)
               and tgfoid in ('pgq.sqltriga'::regproc::oid, 'pgq.logutriga'::regproc::oid)
               and substring(tgargs for (position(E'\\000'::bytea in tgargs) - 1)) = b_queue_name
        loop
            execute 'drop trigger ' || quote_ident(logtrg_name)
                    || ' on ' || londiste.quote_fqname(fq_table_name);
        end loop;

        -- reset data
        update londiste.table_info
            set local = false,
                custom_snapshot = null,

                ---- should we keep those?
                -- skip_truncate = null,
                -- table_attrs = null,
                -- dropped_ddl = null,
                merge_state = null
            where queue_name = i_queue_name
                and table_name = fq_table_name;
    else
        if not pgq_node.is_root_node(i_queue_name) then
            select 400, 'Table not registered locally: ' || fq_table_name into ret_code, ret_note;
            return;
        end if;
    end if;

    if pgq_node.is_root_node(i_queue_name) then
        perform londiste.global_remove_table(i_queue_name, fq_table_name);
        perform londiste.root_notify_change(i_queue_name, 'londiste.remove-table', fq_table_name);
    end if;

    select 200, 'Table removed: ' || fq_table_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;

