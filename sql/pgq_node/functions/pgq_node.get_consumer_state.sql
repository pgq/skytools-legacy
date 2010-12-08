
create or replace function pgq_node.get_consumer_state(
    in i_queue_name text,
    in i_consumer_name text,

    out ret_code int4,
    out ret_note text,

    out node_type text,
    out node_name text,
    out completed_tick bigint,
    out provider_node text,
    out provider_location text,
    out paused boolean,
    out uptodate boolean,
    out cur_error text
) returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.get_consumer_state(2)
--
--      Get info for cascaded consumer that targets local node.
--
-- Parameters:
--      i_node_name  - cascaded queue name
--      i_consumer_name - cascaded consumer name
--
-- Returns:
--      node_type - local node type
--      node_name - local node name
--      completed_tick - last committed tick
--      provider_node - provider node name
--      provider_location - connect string to provider node
--      paused - this node should not do any work
--      uptodate - if consumer has loaded last changes
--      cur_error - failure reason
-- ----------------------------------------------------------------------
begin
    select n.node_type, n.node_name
      into node_type, node_name
      from pgq_node.node_info n
    where n.queue_name = i_queue_name;
    if not found then
        select 404, 'Unknown queue: ' || i_queue_name
          into ret_code, ret_note;
        return;
    end if;
    select s.last_tick_id, s.provider_node, s.paused, s.uptodate, s.cur_error
      into completed_tick, provider_node, paused, uptodate, cur_error
      from pgq_node.local_state s
     where s.queue_name = i_queue_name
       and s.consumer_name = i_consumer_name;
    if not found then
        select 404, 'Unknown consumer: ' || i_queue_name || '/' || i_consumer_name
          into ret_code, ret_note;
        return;
    end if;
    select 100, 'Ok', p.node_location
      into ret_code, ret_note, provider_location
      from pgq_node.node_location p
     where p.queue_name = i_queue_name
      and p.node_name = provider_node;
    if not found then
        select 404, 'Unknown provider node: ' || i_queue_name || '/' || provider_node
          into ret_code, ret_note;
        return;
    end if;
    return;
end;
$$ language plpgsql security definer;

