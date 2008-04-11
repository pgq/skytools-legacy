create or replace function londiste.node_add_table(
    in i_set_name       text,
    in i_table_name     text,
    out ret_code        int4,
    out ret_desc        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.node_add_table(2)
--
--      Register table on Londiste node.
--
-- Returns:
--      200 - Ok
--      400 - No such set
-- ----------------------------------------------------------------------
declare
    col_types text;
    fq_table_name text;
    new_state text;
begin
    fq_table_name := londiste.make_fqname(i_table_name);
    col_types := londiste.find_column_types(fq_table_name);
    if position('k' in col_types) < 1 then
        select 400, 'Primary key missing on table: ' || fq_table_name into ret_code, ret_desc;
        return;
    end if;

    perform 1 from pgq_set.set_info where set_name = i_set_name;
    if not found then
        select 400, 'No such set: ' || i_set_name into ret_code, ret_desc;
        return;
    end if;

    perform 1 from londiste.node_table where set_name = i_set_name and table_name = fq_table_name;
    if found then
        select 200, 'Table already added: ' || fq_table_name into ret_code, ret_desc;
        return;
    end if;

    if pgq_set.is_root(i_set_name) then
        select * into ret_code, ret_desc
            from londiste.set_add_table(i_set_name, fq_table_name);
        if ret_code <> 200 then
            return;
        end if;
        new_state := 'ok';
        perform londiste.root_notify_change(i_set_name, 'add-table', fq_table_name);
    else
        perform 1 from londiste.set_table where set_name = i_set_name and table_name = fq_table_name;
        if not found then
            select 400, 'Table not registered in set: ' || fq_table_name into ret_code, ret_desc;
            return;
        end if;
        new_state := NULL;
    end if;

    insert into londiste.node_table (set_name, table_name, merge_state)
        values (i_set_name, fq_table_name, new_state);

    for ret_code, ret_desc in
        select f.ret_code, f.ret_desc
        from londiste.node_prepare_triggers(i_set_name, fq_table_name) f
    loop
        if ret_code > 299 then
            return;
        end if;
    end loop;

    for ret_code, ret_desc in
        select f.ret_code, f.ret_desc
        from londiste.node_refresh_triggers(i_set_name, fq_table_name) f
    loop
        if ret_code > 299 then
            return;
        end if;
    end loop;

    select 200, 'Table added: ' || fq_table_name into ret_code, ret_desc;
    return;
end;
$$ language plpgsql strict;

