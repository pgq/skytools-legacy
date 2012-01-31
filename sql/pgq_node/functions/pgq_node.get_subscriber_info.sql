
create or replace function pgq_node.get_subscriber_info(
    in i_queue_name text,

    out node_name text,
    out worker_name text,
    out node_watermark int8)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.get_subscriber_info(1)
--
--      Get subscriber list for the local node.
--
--      It may be out-of-date, due to in-progress
--      administrative change.
--      Node's local provider info ( pgq_node.get_node_info() or pgq_node.get_worker_state(1) )
--      is the authoritative source.
--
-- Parameters:
--      i_queue_name  - cascaded queue name
--
-- Returns:
--      node_name       - node name that uses current node as provider
--      worker_name     - consumer that maintains remote node
--      local_watermark - lowest tick_id on subscriber
-- ----------------------------------------------------------------------
declare
    _watermark_name text;
begin
    for node_name, worker_name, _watermark_name in
        select s.subscriber_node, s.worker_name, s.watermark_name
          from pgq_node.subscriber_info s
         where s.queue_name = i_queue_name
         order by 1
    loop
        select last_tick into node_watermark
            from pgq.get_consumer_info(i_queue_name, _watermark_name);
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer;

