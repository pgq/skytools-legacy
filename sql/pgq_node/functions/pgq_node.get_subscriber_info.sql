
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
--      administrative change.  Node's local provider info
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
begin
    for node_name, worker_name, node_watermark in
        select s.subscriber_node, s.worker_name,
               (select last_tick from pgq.get_consumer_info(i_queue_name, s.watermark_name)) as wm_pos
          from pgq_node.subscriber_info s
         where s.queue_name = i_queue_name
         order by 1
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer;

