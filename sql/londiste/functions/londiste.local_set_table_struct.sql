
create or replace function londiste.local_set_table_struct(
    in i_queue_name text,
    in i_table_name text,
    in i_dropped_ddl text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_set_table_struct(3)
--
--      Store dropped table struct temporarily.
--
-- Parameters:
--      i_queue_name    - cascaded queue name
--      i_table         - table name
--      i_dropped_ddl   - merge state
-- ----------------------------------------------------------------------
begin
    update londiste.table_info
        set dropped_ddl = i_dropped_ddl
      where queue_name = i_queue_name
        and table_name = i_table_name
        and local;
    if found then
        select 200, 'Table struct stored'
            into ret_code, ret_note;
    else
        select 404, 'no such local table: '||i_table_name
            into ret_code, ret_note;

    end if;
    return;
end;
$$ language plpgsql;

