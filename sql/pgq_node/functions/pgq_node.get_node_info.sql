
create or replace function pgq_node.get_node_info(
    in i_queue_name text,

    out ret_code int4,
    out ret_note text,
    out node_type text,
    out node_name text,
    out global_watermark bigint,
    out local_watermark bigint,
    out provider_node text,
    out provider_location text,

    out combined_queue text,
    out combined_type text,

    out worker_name text,
    out worker_paused bool,
    out worker_uptodate bool,
    out worker_last_tick bigint
) returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.get_node_info(1)
--
--      Get local node info for cascaded queue.
--
-- Parameters:
--      i_queue_name  - cascaded queue name
--
-- Returns:
--      node_type - local node type
--      node_name - local node name
--      global_watermark - queue's global watermark
--      local_watermark - queue's local watermark, for this and below nodes
--      provider_node - provider node name
--      provider_location - provider connect string
--      combined_queue - queue name for target set
--      combined_type - node type of target setA
--      worker_name - consumer name that maintains this node
--      worker_paused - is worker paused
--      worker_uptodate - is worker seen the changes
--      worker_last_tick - last committed tick_id by worker
-- ----------------------------------------------------------------------
declare
    sql text;
begin
    select 100, 'Ok', n.node_type, n.node_name, g.last_tick,
           c.node_type, c.queue_name, w.provider_node, l.node_location,
           n.worker_name, w.paused, w.uptodate, w.last_tick_id
      into ret_code, ret_note, node_type, node_name, global_watermark,
           combined_type, combined_queue, provider_node, provider_location,
           worker_name, worker_paused, worker_uptodate, worker_last_tick
      from pgq_node.node_info n
           left join pgq_node.node_info c on (c.queue_name = n.combined_queue)
           left join pgq_node.local_state w on (w.queue_name = n.queue_name and w.consumer_name = n.worker_name)
           left join pgq_node.node_location l on (l.queue_name = w.queue_name and l.node_name = w.provider_node)
           left join pgq.get_consumer_info(i_queue_name, '.global_watermark') g on (g.queue_name = n.queue_name)
      where n.queue_name = i_queue_name;
    if not found then
        select 404, 'Unknown queue: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;

    if node_type in ('root', 'branch') then
        select min(last_tick) into local_watermark
          from pgq.get_consumer_info(i_queue_name)
         where consumer_name <> '.global_watermark';
        if local_watermark is null then
            select t.tick_id into local_watermark
              from pgq.tick t, pgq.queue q
             where t.tick_queue = q.queue_id
               and q.queue_name = i_queue_name
             order by 1 desc
             limit 1;
        end if;
    else
        local_watermark := worker_last_tick;
    end if;
    return;
end;
$$ language plpgsql security definer;

