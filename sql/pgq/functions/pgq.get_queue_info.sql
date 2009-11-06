create or replace function pgq.get_queue_info(
    out queue_name                  text,
    out queue_ntables               integer,
    out queue_cur_table             integer,
    out queue_rotation_period       interval,
    out queue_switch_time           timestamptz,
    out queue_external_ticker       boolean,
    out queue_ticker_paused         boolean,
    out queue_ticker_max_count      integer,
    out queue_ticker_max_lag        interval,
    out queue_ticker_idle_period    interval,
    out ticker_lag                  interval,
    out ev_per_sec                  float8,
    out ev_new                bigint)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_queue_info(0)
--
--      Get info about all queues.
--
-- Returns:
--      List of pgq.ret_queue_info records.
-- ----------------------------------------------------------------------
begin
    for queue_name, queue_ntables, queue_cur_table, queue_rotation_period,
        queue_switch_time, queue_external_ticker, queue_ticker_paused,
        queue_ticker_max_count, queue_ticker_max_lag, queue_ticker_idle_period,
        ticker_lag, ev_per_sec, ev_new
    in select
        f.queue_name, f.queue_ntables, f.queue_cur_table, f.queue_rotation_period,
        f.queue_switch_time, f.queue_external_ticker, f.queue_ticker_paused,
        f.queue_ticker_max_count, f.queue_ticker_max_lag, f.queue_ticker_idle_period,
        f.ticker_lag, f.ev_per_sec, f.ev_new
        from pgq.get_queue_info(null) f
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql;

create or replace function pgq.get_queue_info(
    in i_queue_name                 text,
    out queue_name                  text,
    out queue_ntables               integer,
    out queue_cur_table             integer,
    out queue_rotation_period       interval,
    out queue_switch_time           timestamptz,
    out queue_external_ticker       boolean,
    out queue_ticker_paused         boolean,
    out queue_ticker_max_count      integer,
    out queue_ticker_max_lag        interval,
    out queue_ticker_idle_period    interval,
    out ticker_lag                  interval,
    out ev_per_sec                  float8,
    out ev_new                bigint)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_queue_info(1)
--
--      Get info about particular queue.
--
-- Returns:
--      One pgq.ret_queue_info record.
-- ----------------------------------------------------------------------
begin
    for queue_name, queue_ntables, queue_cur_table, queue_rotation_period,
        queue_switch_time, queue_external_ticker, queue_ticker_paused,
        queue_ticker_max_count, queue_ticker_max_lag, queue_ticker_idle_period,
        ticker_lag, ev_per_sec, ev_new
    in select
        q.queue_name, q.queue_ntables, q.queue_cur_table,
        q.queue_rotation_period, q.queue_switch_time,
        q.queue_external_ticker, q.queue_ticker_paused,
        q.queue_ticker_max_count, q.queue_ticker_max_lag,
        q.queue_ticker_idle_period,
        (select current_timestamp - tick_time
           from pgq.tick where tick_queue = queue_id
          order by tick_queue desc, tick_id desc limit 1),
        case when ht.tick_time < top.tick_time
             then (top.tick_event_seq - ht.tick_event_seq) / extract(epoch from (top.tick_time - ht.tick_time))
             else null end,
        pgq.seq_getval(q.queue_event_seq) - top.tick_event_seq
        from pgq.queue q
          left join pgq.tick top
            on (top.tick_queue = q.queue_id
                and top.tick_id = (select tmp.tick_id from pgq.tick tmp
                                    where tmp.tick_queue = q.queue_id
                                    order by tmp.tick_queue desc, tmp.tick_id desc
                                    limit 1))
          left join pgq.tick ht
            on (ht.tick_queue = q.queue_id
                and ht.tick_id = (select tmp2.tick_id from pgq.tick tmp2
                                   where tmp2.tick_queue = q.queue_id
                                     and tmp2.tick_id >= top.tick_id - 20
                                   order by tmp2.tick_queue asc, tmp2.tick_id asc
                                   limit 1))
        where (i_queue_name is null or q.queue_name = i_queue_name)
        order by q.queue_name
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql;

