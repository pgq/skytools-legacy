create or replace function londiste.local_change_handler(
    in i_queue_name     text,
    in i_table_name     text,
    in i_trg_args       text[],
    in i_table_attrs    text,
    out ret_code        int4,
    out ret_note        text)
as $$
----------------------------------------------------------------------------------------------------
-- Function: londiste.local_change_handler(4)
--
--     Change handler and rebuild trigger if needed
--
-- Parameters:
--      i_queue_name  - set name
--      i_table_name  - table name
--      i_trg_args    - args to trigger
--      i_table_attrs - args to python handler
--
-- Returns:
--      200 - OK
--      400 - No such set
--      404 - Table not found
--
----------------------------------------------------------------------------------------------------
declare
    _dest_table text;
    _desc text;
    _node record;
begin
    -- get node info
    select * from pgq_node.get_node_info(i_queue_name) into _node;
    if not found or _node.ret_code >= 400 then
        select 400, 'No such set: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;

    -- update table_attrs with new handler info
    select f.ret_code, f.ret_note
        from londiste.local_set_table_attrs(i_queue_name, i_table_name, i_table_attrs) f
        into ret_code, ret_note;
    if ret_code <> 200 then
        return;
    end if;

    -- get destination table name for use in trigger creation
    select coalesce(ti.dest_table, i_table_name)
        from londiste.table_info ti
        where queue_name = i_queue_name
        and table_name = i_table_name
        and local
        into _dest_table;

    -- replace the trigger if needed
    select f.ret_code, f.ret_note
        from londiste.create_trigger(i_queue_name, i_table_name, i_trg_args, _dest_table, _node.node_type) f
        into ret_code, ret_note;

    if _dest_table = i_table_name then
        _desc := i_table_name;
    else
        _desc := i_table_name || '(' || _dest_table || ')';
    end if;

    if ret_code > 299 then
        ret_note := 'Trigger creation failed for table ' || _desc || ': ' || ret_note;
        return;
    elsif ret_code = 201 then
        select 200, 'Table handler updated with no triggers: ' || _desc
            into ret_code, ret_note;
        return;
    end if;

    select 200, 'Handler changed for table: ' || _desc
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql;
