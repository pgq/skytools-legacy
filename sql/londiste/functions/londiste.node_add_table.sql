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
        select 200, 'OK, already added: ' || fq_table_name into ret_code, ret_desc;
        return;
    end if;

    insert into londiste.node_table (set_name, table_name)
        values (i_set_name, fq_table_name);
    select 200, 'OK' into ret_code, ret_desc;
    return;
end;
$$ language plpgsql strict;

