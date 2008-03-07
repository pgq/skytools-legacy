
create or replace function londiste.node_set_skip_truncate(
    i_set_name  text,
    i_table     text,
    i_value     bool)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.node_set_skip_truncate(x)
--
--      Change skip_truncate flag for table.
-- ----------------------------------------------------------------------
begin
    update londiste.node_table
       set skip_truncate = i_value
     where set_name = i_set_name
       and table_name = i_table;
    if not found then
        raise exception 'table not found';
    end if;

    return 1;
end;
$$ language plpgsql;

