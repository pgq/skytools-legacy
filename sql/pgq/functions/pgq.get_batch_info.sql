
create or replace function pgq.get_batch_info(x_batch_id bigint)
returns pgq.ret_batch_info as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_batch_info(1)
--
--      Returns detailed info about a batch.
--
-- Parameters:
--      x_batch_id      - id of a active batch.
--
-- Returns:
--      Info
-- ----------------------------------------------------------------------
declare
    ret  pgq.ret_batch_info%rowtype;
begin
    select queue_name, co_name,
           prev.tick_time as batch_start,
           cur.tick_time as batch_end,
           sub_last_tick, sub_next_tick,
           current_timestamp - cur.tick_time as lag
        into ret
        from pgq.subscription, pgq.tick cur, pgq.tick prev,
             pgq.queue, pgq.consumer
        where sub_batch = x_batch_id
          and prev.tick_id = sub_last_tick
          and prev.tick_queue = sub_queue
          and cur.tick_id = sub_next_tick
          and cur.tick_queue = sub_queue
          and queue_id = sub_queue
          and co_id = sub_consumer;
    return ret;
end;
$$ language plpgsql security definer;

