
create or replace function londiste.local_set_skip_truncate(
    in i_queue_name text,
    in i_table      text,
    in i_value      bool,
    out ret_code    int4,
    out ret_note    text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_set_skip_truncate(3)
--
--      Change skip_truncate flag for table.
-- ----------------------------------------------------------------------
begin
    update londiste.table_info
       set skip_truncate = i_value
     where queue_name = i_queue_name
       and table_name = i_table;
    if found then
        select 200, 'skip_truncate=' || i_value::text
            into ret_code, ret_note;
    else
        select 404, 'table not found: ' || i_table
            into ret_code, ret_note;
    end if;
    return;
end;
$$ language plpgsql;

