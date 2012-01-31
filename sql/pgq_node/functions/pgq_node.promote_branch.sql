
create or replace function pgq_node.promote_branch(
    in i_queue_name text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.promote_branch(1)
--
--      Promote branch node to root.
--
-- Parameters:
--      i_queue_name  - queue name
--
-- Returns:
--      200 - success
--      404 - node not initialized for queue 
--      301 - node is not branch
-- ----------------------------------------------------------------------
declare
    n_name      text;
    n_type      text;
    w_name      text;
    last_tick   bigint;
    sql         text;
begin
    select node_name, node_type, worker_name into n_name, n_type, w_name
        from pgq_node.node_info
        where queue_name = i_queue_name;
    if not found then
        select 404, 'Node not initialized for queue: ' || i_queue_name
          into ret_code, ret_note;
        return;
    end if;

    if n_type != 'branch' then
        select 301, 'Node not branch'
          into ret_code, ret_note;
        return;
    end if;

    update pgq.queue
        set queue_disable_insert = false,
            queue_external_ticker = false
        where queue_name = i_queue_name;

    -- change type, point worker to itself
    select t.tick_id into last_tick
        from pgq.tick t, pgq.queue q
        where q.queue_name = i_queue_name
            and t.tick_queue = q.queue_id
        order by t.tick_queue desc, t.tick_id desc
        limit 1;

    -- make tick seq larger than last tick
    perform pgq.seq_setval(queue_tick_seq, last_tick)
        from pgq.queue where queue_name = i_queue_name;

    update pgq_node.node_info
        set node_type = 'root'
        where queue_name = i_queue_name;

    update pgq_node.local_state
        set provider_node = n_name,
            last_tick_id = last_tick,
            uptodate = false
        where queue_name = i_queue_name
            and consumer_name = w_name;

    select 200, 'Branch node promoted to root'
      into ret_code, ret_note;

    return;
end;
$$ language plpgsql security definer;

