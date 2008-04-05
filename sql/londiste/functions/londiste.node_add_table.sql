create or replace function londiste.node_add_table(
    in i_set_name       text,
    in i_table_name     text,
    out ret_code        int4,
    out ret_desc        text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.node_add_table(x)
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
begin
    fq_table_name := londiste.make_fqname(i_table_name);
    col_types := londiste.find_column_types(fq_table_name);
    if position('k' in col_types) < 1 then
        raise exception 'need key column';
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
        perform londiste.set_add_table(i_set_name, fq_table_name);
    else
        perform 1 from londiste.set_table where set_name = i_set_name and table_name = fq_table_name;
        if not found then
            select 400, 'Table not registered in set: ' || fq_table_name into ret_code, ret_desc;
            return;
        end if;
    end if;

    if pgq_set.is_root(i_set_name) then
        select * into ret_code, ret_desc
            from londiste.set_add_table(i_set_name, fq_table_name);
        if ret_code <> 200 then
            return;
        end if;
    end if;

    insert into londiste.node_table (set_name, table_name)
        values (i_set_name, fq_table_name);
    select 200, 'Table added: ' || fq_table_name into ret_code, ret_desc;
    return;
end;
$$ language plpgsql strict;

