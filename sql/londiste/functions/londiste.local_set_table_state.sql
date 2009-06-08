
create or replace function londiste.local_set_table_state(
    in i_queue_name text,
    in i_table_name text,
    in i_snapshot text,
    in i_merge_state text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_set_table_state(4)
--
--      Change table state.
--
-- Parameters:
--      i_queue_name    - cascaded queue name
--      i_table         - table name
--      i_snapshot      - optional remote snapshot info
--      i_merge_state   - merge state
-- ----------------------------------------------------------------------
declare
    _tbl text;
begin
    _tbl = londiste.make_fqname(i_table_name);

    update londiste.table_info
        set custom_snapshot = i_snapshot,
            merge_state = i_merge_state
      where queue_name = i_queue_name
        and table_name = _tbl
        and local;
    if not found then
        select 404, 'No such table: ' || _tbl
            into ret_code, ret_note;
        return;
    end if;

    select 200, 'Table ' || _tbl || ' state set to '
            || coalesce(quote_literal(i_merge_state), 'NULL')
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql;

