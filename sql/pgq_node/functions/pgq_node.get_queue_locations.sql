
create or replace function pgq_node.get_queue_locations(
    in i_queue_name text,

    out node_name text,
    out node_location text,
    out dead boolean
) returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.get_queue_locations(1)
--
--      Get node list for the queue.
--
-- Parameters:
--      i_queue_name    - queue name
--
-- Returns:
--      node_name       - node name
--      node_location   - libpq connect string for the node
--      dead            - whether the node should be considered dead
-- ----------------------------------------------------------------------
begin
    for node_name, node_location, dead in
        select l.node_name, l.node_location, l.dead
          from pgq_node.node_location l
         where l.queue_name = i_queue_name
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer;

