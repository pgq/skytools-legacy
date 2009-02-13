
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
    out uptodate boolean
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
-- ----------------------------------------------------------------------
begin
    select 200, 'Ok', n.node_type, n.node_name, s.last_tick_id,
           s.provider_node, p.node_location, s.paused, s.uptodate
      into ret_code, ret_note, node_type, node_name, completed_tick,
           provider_node, provider_location, paused, uptodate
      from pgq_node.node_info n, pgq_node.local_state s, pgq_node.node_location p
     where n.queue_name = i_queue_name
       and s.queue_name = n.queue_name
       and s.consumer_name = i_consumer_name
       and p.queue_name = n.queue_name
       and p.node_name = s.provider_node;
    if not found then
        select 404, 'Unknown consumer: ' || i_queue_name || '/' || i_consumer_name
          into ret_code, ret_note;
    end if;
    return;
end;
$$ language plpgsql security definer;

