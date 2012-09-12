
create or replace function pgq_node.demote_root(
    in i_queue_name text,
    in i_step int4,
    in i_new_provider text,
    out ret_code int4,
    out ret_note text,
    out last_tick int8)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.demote_root(3)
--
--      Multi-step root demotion to branch.
--
--      Must be be called for each step in sequence:
--
--      Step 1 - disable writing to queue.
--      Step 2 - wait until writers go away, do tick.
--      Step 3 - change type, register.
--
-- Parameters:
--      i_queue_name    - queue name
--      i_step          - step number
--      i_new_provider  - new provider node
-- Returns:
--      200 - success
--      404 - node not initialized for queue 
--      301 - node is not root
-- ----------------------------------------------------------------------
declare
    n_type      text;
    w_name      text;
    sql         text;
    ev_id       int8;
    ev_tbl      text;
begin
    select node_type, worker_name into n_type, w_name
        from pgq_node.node_info
        where queue_name = i_queue_name;
    if not found then
        select 404, 'Node not initialized for queue: ' || i_queue_name
          into ret_code, ret_note;
        return;
    end if;

    if n_type != 'root' then
        select 301, 'Node not root'
          into ret_code, ret_note;
        return;
    end if;
    if i_step > 1 then
        select queue_data_pfx
            into ev_tbl
            from pgq.queue
            where queue_name = i_queue_name
                and queue_disable_insert
                and queue_external_ticker;
        if not found then
            raise exception 'steps in wrong order';
        end if;
    end if;

    if i_step = 1 then
        update pgq.queue
            set queue_disable_insert = true,
                queue_external_ticker = true
            where queue_name = i_queue_name;
        if not found then
            select 404, 'Huh, no queue?: ' || i_queue_name
              into ret_code, ret_note;
            return;
        end if;
        select 200, 'Step 1: Writing disabled for: ' || i_queue_name
          into ret_code, ret_note;
    elsif i_step = 2 then
        set local session_replication_role = 'replica';

        -- lock parent table to stop updates, allow reading
        sql := 'lock table ' || ev_tbl || ' in exclusive mode';
        execute sql;
        

        select nextval(queue_tick_seq), nextval(queue_event_seq)
            into last_tick, ev_id
            from pgq.queue
            where queue_name = i_queue_name;

        perform pgq.ticker(i_queue_name, last_tick, now(), ev_id);

        select 200, 'Step 2: Inserted last tick: ' || i_queue_name
            into ret_code, ret_note;
    elsif i_step = 3 then
        -- change type, point worker to new provider
        select t.tick_id into last_tick
            from pgq.tick t, pgq.queue q
            where q.queue_name = i_queue_name
                and t.tick_queue = q.queue_id
            order by t.tick_queue desc, t.tick_id desc
            limit 1;
        update pgq_node.node_info
            set node_type = 'branch'
            where queue_name = i_queue_name;
        update pgq_node.local_state
            set provider_node = i_new_provider,
                last_tick_id = last_tick,
                uptodate = false
            where queue_name = i_queue_name
                and consumer_name = w_name;
        select 200, 'Step 3: Demoted root to branch: ' || i_queue_name
          into ret_code, ret_note;
    else
        raise exception 'incorrect step number';
    end if;
    return;
end;
$$ language plpgsql security definer;

