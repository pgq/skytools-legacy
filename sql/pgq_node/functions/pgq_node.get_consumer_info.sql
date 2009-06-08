
create or replace function pgq_node.get_consumer_info(
    in i_queue_name text,

    out consumer_name text,
    out provider_node text,
    out last_tick_id int8,
    out paused boolean,
    out uptodate boolean,
    out cur_error text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.get_consumer_info(1)
--
--      Get consumer list that work on the local node.
--
-- Parameters:
--      i_queue_name  - cascaded queue name
--
-- Returns:
--      consumer_name   - cascaded consumer name
--      provider_node   - node from where the consumer reads from
--      last_tick_id    - last committed tick
--      paused          - if consumer is paused
--      uptodate        - if consumer is uptodate
--      cur_error       - failure reason
-- ----------------------------------------------------------------------
begin
    for consumer_name, provider_node, last_tick_id, paused, uptodate, cur_error in
        select s.consumer_name, s.provider_node, s.last_tick_id,
               s.paused, s.uptodate, s.cur_error
            from pgq_node.local_state s
            where s.queue_name = i_queue_name
            order by 1
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer;

