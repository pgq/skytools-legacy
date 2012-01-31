
create or replace function pgq_node.set_node_attrs(
    in i_queue_name text,
    in i_node_attrs text,
    out ret_code int4,
    out ret_note  text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.create_attrs(2)
--
--      Set node attributes.
--
-- Parameters:
--      i_node_name - cascaded queue name
--      i_node_attrs - urlencoded node attrs
--
-- Returns:
--      200 - ok
--      404 - node not found
-- ----------------------------------------------------------------------
begin
    update pgq_node.node_info
        set node_attrs = i_node_attrs
        where queue_name = i_queue_name;
    if not found then
        select 404, 'Node not found' into ret_code, ret_note;
        return;
    end if;

    select 200, 'Node attributes updated'
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

