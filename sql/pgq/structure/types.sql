
create type pgq.ret_queue_info as (
    queue_name                  text,
    queue_ntables               integer,
    queue_cur_table             integer,
    queue_rotation_period       interval,
    queue_switch_time           timestamptz,
    queue_external_ticker       boolean,
    queue_ticker_max_count      integer,
    queue_ticker_max_lag        interval,
    queue_ticker_idle_period    interval,
    ticker_lag                  interval
);

create type pgq.ret_consumer_info as (
    queue_name      text,
    consumer_name   text,
    lag             interval,
    last_seen       interval,
    last_tick       bigint,
    current_batch   bigint,
    next_tick       bigint
);

create type pgq.ret_batch_info as (
    queue_name      text,
    consumer_name   text,
    batch_start     timestamptz,
    batch_end       timestamptz,
    prev_tick_id    bigint,
    tick_id         bigint,
    lag             interval
);


create type pgq.ret_batch_event as (
    ev_id	    bigint,
    ev_time         timestamptz,

    ev_txid         bigint,
    ev_retry        int4,

    ev_type         text,
    ev_data         text,
    ev_extra1       text,
    ev_extra2       text,
    ev_extra3       text,
    ev_extra4       text
);

