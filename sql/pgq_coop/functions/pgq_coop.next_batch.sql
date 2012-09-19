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
--	Result NULL means nothing to work with, for a moment
--
-- Parameters:
--	i_queue_name		- Name of the queue
--	i_consumer_name		- Name of the consumer
--	i_subconsumer_name	- Name of the subconsumer
--
-- Calls:
--      pgq.register_consumer(i_queue_name, i_consumer_name)
--      pgq.register_consumer(i_queue_name, _subcon_name);
--
-- Tables directly manipulated:
--      update - pgq.subscription
-- 
-- ----------------------------------------------------------------------
begin
    return pgq_coop.next_batch_custom(i_queue_name, i_consumer_name, i_subconsumer_name, NULL, NULL, NULL, NULL);
end;
$$ language plpgsql;

create or replace function pgq_coop.next_batch(
    i_queue_name text,
    i_consumer_name text,
    i_subconsumer_name text,
    i_dead_interval interval)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq_coop.next_batch(4)
--
--	Makes next block of events active
--
--      If i_dead_interval is set, other subconsumers are checked for
--      inactivity.  If some subconsumer has active batch, but has
--      been inactive more than i_dead_interval, the batch is taken over.
--
--	Result NULL means nothing to work with, for a moment
--
-- Parameters:
--	i_queue_name		- Name of the queue
--	i_consumer_name		- Name of the consumer
--	i_subconsumer_name	- Name of the subconsumer
--      i_dead_interval         - Take over other subconsumer batch if inactive
-- ----------------------------------------------------------------------
begin
    return pgq_coop.next_batch_custom(i_queue_name, i_consumer_name, i_subconsumer_name, NULL, NULL, NULL, i_dead_interval);
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
--	Result NULL means nothing to work with, for a moment
--
-- Parameters:
--	i_queue_name		- Name of the queue
--	i_consumer_name		- Name of the consumer
--	i_subconsumer_name	- Name of the subconsumer
--      i_min_lag           - Consumer wants events older than that
--      i_min_count         - Consumer wants batch to contain at least this many events
--      i_min_interval      - Consumer wants batch to cover at least this much time
-- ----------------------------------------------------------------------
begin
    return pgq_coop.next_batch_custom(i_queue_name, i_consumer_name, i_subconsumer_name,
                                      i_min_lag, i_min_count, i_min_interval, NULL);
end;
$$ language plpgsql;

create or replace function pgq_coop.next_batch_custom(
    i_queue_name text,
    i_consumer_name text,
    i_subconsumer_name text,
    i_min_lag interval,
    i_min_count int4,
    i_min_interval interval,
    i_dead_interval interval)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq_coop.next_batch_custom(7)
--
--      Makes next block of events active.  Block size can be tuned
--      with i_min_count, i_min_interval parameters.  Events age can
--      be tuned with i_min_lag.
--
--      If i_dead_interval is set, other subconsumers are checked for
--      inactivity.  If some subconsumer has active batch, but has
--      been inactive more than i_dead_interval, the batch is taken over.
--
--	Result NULL means nothing to work with, for a moment
--
-- Parameters:
--      i_queue_name        - Name of the queue
--      i_consumer_name     - Name of the consumer
--      i_subconsumer_name  - Name of the subconsumer
--      i_min_lag           - Consumer wants events older than that
--      i_min_count         - Consumer wants batch to contain at least this many events
--      i_min_interval      - Consumer wants batch to cover at least this much time
--      i_dead_interval     - Take over other subconsumer batch if inactive
-- Calls:
--      pgq.register_subconsumer(i_queue_name, i_consumer_name, i_subconsumer_name)
--      pgq.next_batch_custom(i_queue_name, i_consumer_name, i_min_lag, i_min_count, i_min_interval)
-- Tables directly manipulated:
--      update - pgq.subscription
-- ----------------------------------------------------------------------
declare
    _queue_id integer;
    _consumer_id integer;
    _subcon_id integer;
    _batch_id bigint;
    _prev_tick bigint;
    _cur_tick bigint;
    _sub_id integer;
    _dead record;
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
        perform pgq_coop.register_subconsumer(i_queue_name, i_consumer_name, i_subconsumer_name);
        -- fetch the data again
        select q.queue_id, c.co_id, s.sub_next_tick
            into _queue_id, _consumer_id, _cur_tick
            from pgq.queue q, pgq.consumer c, pgq.subscription s
            where q.queue_name = i_queue_name
              and c.co_name = i_consumer_name
              and s.sub_queue = q.queue_id
              and s.sub_consumer = c.co_id;
    end if;
    if _cur_tick is not null then
        raise exception 'main consumer has batch open?';
    end if;

    -- automatically register subconsumers
    perform 1 from pgq.subscription s, pgq.consumer c, pgq.queue q
        where q.queue_name = i_queue_name
          and s.sub_queue = q.queue_id
          and s.sub_consumer = c.co_id
          and c.co_name = i_consumer_name || '.' || i_subconsumer_name;
    if not found then
        perform pgq_coop.register_subconsumer(i_queue_name, i_consumer_name, i_subconsumer_name);
    end if;

    -- fetch subconsumer details
    select s.sub_batch, sc.co_id, s.sub_id
        into _batch_id, _subcon_id, _sub_id
        from pgq.subscription s, pgq.consumer sc
        where sub_queue = _queue_id
          and sub_consumer = sc.co_id
          and sc.co_name = i_consumer_name || '.' || i_subconsumer_name;
    if not found then
        raise exception 'subconsumer not found';
    end if;

    -- is there a batch already active
    if _batch_id is not null then
        update pgq.subscription set sub_active = now()
            where sub_queue = _queue_id
              and sub_consumer = _subcon_id;
        return _batch_id;
    end if;

    -- help dead comrade
    if i_dead_interval is not null then

        -- check if some other subconsumer has died
        select s.sub_batch, s.sub_consumer, s.sub_last_tick, s.sub_next_tick
            into _dead
            from pgq.subscription s
            where s.sub_queue = _queue_id
              and s.sub_id = _sub_id
              and s.sub_consumer <> _subcon_id
              and s.sub_consumer <> _consumer_id
              and sub_active < now() - i_dead_interval
            limit 1;

        if found then
            -- unregister old consumer
            delete from pgq.subscription
                where sub_queue = _queue_id
                  and sub_consumer = _dead.sub_consumer;

            -- if dead consumer had batch, copy it over and return
            if _dead.sub_batch is not null then
                update pgq.subscription
                    set sub_batch = _dead.sub_batch,
                        sub_last_tick = _dead.sub_last_tick,
                        sub_next_tick = _dead.sub_next_tick,
                        sub_active = now()
                    where sub_queue = _queue_id
                      and sub_consumer = _subcon_id;

                return _dead.sub_batch;
            end if;
        end if;
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

