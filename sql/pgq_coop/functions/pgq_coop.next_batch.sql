create or replace function pgq_coop.next_batch(
    i_queue_name text,
    i_consumer_name text,
    i_subconsumer_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq_coop.next_batch(3)
--
--	Makes next block of events active
--
--	NULL means nothing to work with, for a moment
--
-- Parameters:
--	i_queue_name		- Name of the queue
--	i_consumer_name		- Name of the consumer
--	i_subconsumer_name	- Name of the subconsumer
-- ----------------------------------------------------------------------
begin
    return pgq_coop.next_batch_custom(i_queue_name, i_consumer_name, i_subconsumer_name, NULL, NULL, NULL);
end;
$$ language plpgsql;

create or replace function pgq_coop.next_batch_custom(
    i_queue_name text,
    i_consumer_name text,
    i_subconsumer_name text,
    i_min_lag interval,
    i_min_count int4,
    i_min_interval interval)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq_coop.next_batch_custom(6)
--
--      Makes next block of events active.  Block size can be tuned
--      with i_min_count, i_min_interval parameters.  Events age can
--      be tuned with i_min_lag.
--
--	NULL means nothing to work with, for a moment
--
-- Parameters:
--	i_queue_name		- Name of the queue
--	i_consumer_name		- Name of the consumer
--	i_subconsumer_name	- Name of the subconsumer
--      i_min_lag           - Consumer wants events older than that
--      i_min_count         - Consumer wants batch to contain at least this many events
--      i_min_interval      - Consumer wants batch to cover at least this much time
-- ----------------------------------------------------------------------
declare
    _queue_id integer;
    _consumer_id integer;
    _subcon_id integer;
    _batch_id bigint;
    _prev_tick bigint;
    _cur_tick bigint;
begin
    -- fetch master consumer details, lock the row
    select q.queue_id, c.co_id, s.sub_next_tick
        into _queue_id, _consumer_id, _cur_tick
        from pgq.queue q, pgq.consumer c, pgq.subscription s
        where q.queue_name = i_queue_name
          and c.co_name = i_consumer_name
          and s.sub_queue = q.queue_id
          and s.sub_consumer = c.co_id
        for update of s;
    if not found then
        raise exception 'main consumer not found';
    end if;
    if _cur_tick is not null then
        raise exception 'main consumer has batch open?';
    end if;

    -- fetch subconsumer details
    select s.sub_batch, sc.co_id
        into _batch_id, _subcon_id
        from pgq.subscription s, pgq.consumer sc
        where sub_queue = _queue_id
          and sub_consumer = sc.co_id
          and sc.co_name = i_consumer_name || '.' || i_subconsumer_name;
    if not found then
        raise exception 'subconsumer not found';
    end if;

    -- is there a batch already active
    if _batch_id is not null then
        return _batch_id;
    end if;

    -- get a new batch for the main consumer
    select batch_id, cur_tick_id, prev_tick_id
        into _batch_id, _cur_tick, _prev_tick
        from pgq.next_batch_custom(i_queue_name, i_consumer_name, i_min_lag, i_min_count, i_min_interval);
    if _batch_id is null then
        return null;
    end if;

    -- close batch for main consumer
    update pgq.subscription
       set sub_batch = null,
           sub_active = now(),
           sub_last_tick = sub_next_tick,
           sub_next_tick = null
     where sub_queue = _queue_id
       and sub_consumer = _consumer_id;

    -- copy state into subconsumer row
    update pgq.subscription
        set sub_batch = _batch_id,
            sub_last_tick = _prev_tick,
            sub_next_tick = _cur_tick,
            sub_active = now()
        where sub_queue = _queue_id
          and sub_consumer = _subcon_id;

    return _batch_id;
end;
$$ language plpgsql security definer;

