
create or replace function pgq_set.get_node_info(
    in i_set_name text,

    out node_type text,
    out node_name text,
    out queue_name text,
    out global_watermark bigint,
    out local_watermark bigint,
    out completed_tick bigint,

    out provider_node text,
    out provider_location text,
    out paused boolean,
    out resync boolean,
    out up_to_date boolean,

    out combined_set text,
    out combined_type text,
    out combined_queue text
) returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.get_node_info(1)
--
--      Get local node info for set.
--
-- Parameters:
--      i_set_name  - set name
--
-- Returns:
--      node_type - local node type
--      node_name - local node name
--      queue_name - local queue name used for set
--      global_watermark - set's global watermark
--      local_watermark - set's local watermark, for this and below nodes
--      completed_tick - last committed set's tick
--      provider_node - provider node name
--      provider_location - connect string to provider node
--      paused - this node should not do any work
--      resync - re-register on provider queue (???)
--      up_to_date - if consumer has loaded last changes
--      combined_set - target set name for merge-leaf
--      combined_type - node type of target set
--      combined_queue - queue name for target set
-- ----------------------------------------------------------------------
declare
    sql text;
begin
    select n.node_type, n.node_name, t.tick_id, n.queue_name,
           c.set_name, c.node_type, c.queue_name, n.global_watermark,
           n.provider_node, n.paused, n.resync, n.up_to_date,
           p.node_location
      into node_type, node_name, completed_tick, queue_name,
           combined_set, combined_type, combined_queue, global_watermark,
           provider_node, paused, resync, up_to_date,
           provider_location
      from pgq_set.set_info n
           left join pgq_set.completed_tick t on (t.set_name = n.set_name)
           left join pgq_set.set_info c on (c.set_name = n.combined_set)
           left join pgq_set.member_info p on (p.set_name = n.set_name and p.node_name = n.provider_node)
      where n.set_name = i_set_name;

    select min(u.tick_id) into local_watermark
      from (select tick_id
              from pgq_set.completed_tick
             where set_name = i_set_name
            union all
            select local_watermark as tick_id
              from pgq_set.subscriber_info
             where set_name = i_set_name
            union all
            -- exclude watermark consumer
            select last_tick as tick_id
              from pgq.get_consumer_info(queue_name)
             where consumer_name <> (i_set_name || '_watermark')
            ) u;
    if local_watermark is null and queue_name is not null then
        select t.tick_id into local_watermark
          from pgq.tick t, pgq.queue q
         where t.queue_id = q.queue_id
           and q.queue_name = queue_name
         order by 1 desc
         limit 1;
    end if;
    return;
end;
$$ language plpgsql security definer;

