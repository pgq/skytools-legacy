create or replace function pgq.get_queue_info()
returns setof pgq.ret_queue_info as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_queue_info(0)
--
--      Get info about all queues.
--
-- Returns:
--      List of pgq.ret_queue_info records.
-- ----------------------------------------------------------------------
declare
    q     record;
    ret   pgq.ret_queue_info%rowtype;
begin
    for q in
        select queue_name from pgq.queue order by 1
    loop
        select * into ret from pgq.get_queue_info(q.queue_name);
        return next ret;
    end loop;
    return;
end;
$$ language plpgsql security definer;

create or replace function pgq.get_queue_info(qname text)
returns pgq.ret_queue_info as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_queue_info(1)
--
--      Get info about particular queue.
--
-- Returns:
--      One pgq.ret_queue_info record.
-- ----------------------------------------------------------------------
declare
    ret   pgq.ret_queue_info%rowtype;
begin
    select queue_name, queue_ntables, queue_cur_table,
           queue_rotation_period, queue_switch_time,
           queue_external_ticker,
           queue_ticker_max_count, queue_ticker_max_lag,
           queue_ticker_idle_period,
           (select current_timestamp - tick_time
              from pgq.tick where tick_queue = queue_id
             order by tick_queue desc, tick_id desc limit 1
            ) as ticker_lag
      into ret from pgq.queue where queue_name = qname;
    return ret;
end;
$$ language plpgsql security definer;

