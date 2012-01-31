
create or replace function pgq_node.unregister_consumer(
    in i_queue_name text,
    in i_consumer_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.unregister_consumer(2)
--
--      Unregister cascaded consumer from local node.
--
-- Parameters:
--      i_queue_name - cascaded queue name
--      i_consumer_name - cascaded consumer name
--
-- Returns:
--      ret_code - error code
--      200 - ok
--      404 - no such queue
--      ret_note - description
-- ----------------------------------------------------------------------
begin
    perform 1 from pgq_node.node_info where queue_name = i_queue_name
       for update;
    if not found then
        select 404, 'Unknown queue: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;

    delete from pgq_node.local_state
      where queue_name = i_queue_name
        and consumer_name = i_consumer_name;

    select 200, 'Consumer '||i_consumer_name||' unregistered from '||i_queue_name
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

