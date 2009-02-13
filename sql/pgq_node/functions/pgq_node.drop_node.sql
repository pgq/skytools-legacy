
create or replace function pgq_node.drop_node(
    in i_queue_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.drop_node(1)
--
--      Drop node.
--
-- Parameters:
--      i_queue_name - queue name
--
-- Returns:
--      ret_code - error code
--      ret_note - error description
--
-- Return Codes:
--      200 - Ok
--      404 - No such queue
-- ----------------------------------------------------------------------
declare
    _wm_consumer text;
    _global_wm bigint;
    sub record;
begin
    perform 1 from pgq_node.node_info
      where queue_name = i_queue_name;
    if not found then
        select 404, 'No such queue: ' || i_node_name into ret_code, ret_note;
        return;
    end if;

    perform pgq_node.unsubscribe_node(s.set_name, s.node_name)
       from pgq_node.subscriber_info s
      where set_name = i_node_name;

    delete from pgq_node.completed_tick
     where set_name = i_node_name;

    delete from pgq_node.set_info
     where set_name = i_node_name;

    delete from pgq_node.node_location
     where set_name = i_node_name;

    select 200, 'Node dropped' into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

