
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
    qtbl            text;
    seqname         text;
    tbl             record;
    tbl_oid         oid;
    pgver           integer;
begin
    fq_table_name := londiste.make_fqname(i_table_name);
    qtbl := londiste.quote_fqname(fq_table_name);
    tbl_oid := londiste.find_table_oid(i_table_name);
    show server_version_num into pgver;

    select local, dropped_ddl, merge_state into tbl
        from londiste.table_info
        where queue_name = i_queue_name
          and table_name = fq_table_name
        for update;
    if not found then
        select 400, 'Table not found: ' || fq_table_name into ret_code, ret_note;
        return;
    end if;

    if tbl.local then
        perform londiste.drop_table_triggers(i_queue_name, fq_table_name);

        -- restore dropped ddl
        if tbl.dropped_ddl is not null then
            -- table is not synced, drop data to make restore faster
            if pgver >= 80400 then
                execute 'TRUNCATE ONLY ' || qtbl;
            else
                execute 'TRUNCATE ' || qtbl;
            end if;
            execute tbl.dropped_ddl;
        end if;

        -- reset data
        update londiste.table_info
            set local = false,
                custom_snapshot = null,
                table_attrs = null,
                dropped_ddl = null,
                merge_state = null,
                dest_table = null
            where queue_name = i_queue_name
                and table_name = fq_table_name;

        -- drop dependent sequence
        for seqname in
            select n.nspname || '.' || s.relname
                from pg_catalog.pg_class s,
                     pg_catalog.pg_namespace n,
                     pg_catalog.pg_attribute a
                where a.attrelid = tbl_oid
                    and a.atthasdef
                    and a.atttypid::regtype::text in ('integer', 'bigint')
                    and s.oid = pg_get_serial_sequence(qtbl, a.attname)::regclass::oid
                    and n.oid = s.relnamespace
        loop
            perform londiste.local_remove_seq(i_queue_name, seqname);
        end loop;
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

