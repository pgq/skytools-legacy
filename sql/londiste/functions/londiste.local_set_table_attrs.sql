
create or replace function londiste.local_set_table_attrs(
    in i_queue_name text,
    in i_table_name text,
    in i_table_attrs text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_set_table_attrs(3)
--
--      Store urlencoded table attributes.
--
-- Parameters:
--      i_queue_name    - cascaded queue name
--      i_table         - table name
--      i_table_attrs   - urlencoded attributes
-- ----------------------------------------------------------------------
begin
    update londiste.table_info
        set table_attrs = i_table_attrs
      where queue_name = i_queue_name
        and table_name = i_table_name
        and local;
    if found then
        select 200, i_table_name || ': Table attributes stored'
            into ret_code, ret_note;
    else
        select 404, 'no such local table: ' || i_table_name
            into ret_code, ret_note;
    end if;
    return;
end;
$$ language plpgsql;

