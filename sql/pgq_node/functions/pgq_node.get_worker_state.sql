
create or replace function pgq_node.get_worker_state(
    in i_queue_name text,

    out ret_code int4,
    out ret_note text,

    out node_type text,
    out node_name text,
    out completed_tick bigint,
    out provider_node text,
    out provider_location text,
    out paused boolean,
    out uptodate boolean,
    out cur_error text,

    out worker_name text,
    out global_watermark bigint,
    out local_watermark bigint,
    out local_queue_top bigint,
    out combined_queue text,
    out combined_type text
) returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.get_worker_state(1)
--
--      Get info for consumer that maintains local node.
--
-- Parameters:
--      i_queue_name  - cascaded queue name
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

--      worker_name - consumer name that maintains this node
--      global_watermark - queue's global watermark
--      local_watermark - queue's local watermark, for this and below nodes
--      local_queue_top - last tick in local queue
--      combined_queue - queue name for target set
--      combined_type - node type of target setA
-- ----------------------------------------------------------------------
begin
    select n.node_type, n.node_name, n.worker_name, n.combined_queue
      into node_type, node_name, worker_name, combined_queue
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
       and s.consumer_name = worker_name;
    if not found then
        select 404, 'Unknown consumer: ' || i_queue_name || '/' || worker_name
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

    if combined_queue is not null then
        select n.node_type into combined_type
          from pgq_node.node_info n
         where n.queue_name = get_worker_state.combined_queue;
        if not found then
            select 404, 'Combinde queue node not found: ' || combined_queue
              into ret_code, ret_note;
            return;
        end if;
    end if;

    if node_type in ('root', 'branch') then
        select min(case when consumer_name = '.global_watermark' then null else last_tick end),
               min(case when consumer_name = '.global_watermark' then last_tick else null end)
          into local_watermark, global_watermark
          from pgq.get_consumer_info(i_queue_name);
        if local_watermark is null then
            select t.tick_id into local_watermark
              from pgq.tick t, pgq.queue q
             where t.tick_queue = q.queue_id
               and q.queue_name = i_queue_name
             order by 1 desc
             limit 1;
        end if;

        select tick_id from pgq.tick t, pgq.queue q
         where q.queue_name = i_queue_name
           and t.tick_queue = q.queue_id
         order by t.tick_queue desc, t.tick_id desc
         limit 1 into local_queue_top;
    else
        local_watermark := completed_tick;
    end if;

    return;
end;
$$ language plpgsql security definer;

