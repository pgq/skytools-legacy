create or replace function pgq.next_batch(x_queue_name text, x_consumer_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.next_batch(2)
--
--      Makes next block of events active.
--
--      If it returns NULL, there is no events available in queue.
--      Consumer should sleep a bith then.
--
-- Parameters:
--      x_queue_name        - Name of the queue
--      x_consumer_name     - Name of the consumer
--
-- Returns:
--      Batch ID or NULL if there are no more events available.
-- ----------------------------------------------------------------------
declare
    next_tick       bigint;
    batch_id        bigint;
    errmsg          text;
    sub             record;
begin
    select sub_queue, sub_consumer, sub_id, sub_last_tick, sub_batch into sub
        from pgq.queue q, pgq.consumer c, pgq.subscription s
        where q.queue_name = x_queue_name
          and c.co_name = x_consumer_name
          and s.sub_queue = q.queue_id
          and s.sub_consumer = c.co_id;
    if not found then
        errmsg := 'Not subscriber to queue: '
            || coalesce(x_queue_name, 'NULL')
            || '/'
            || coalesce(x_consumer_name, 'NULL');
        raise exception '%', errmsg;
    end if;

    -- has already active batch
    if sub.sub_batch is not null then
        return sub.sub_batch;
    end if;

    -- find next tick
    select tick_id into next_tick
        from pgq.tick
        where tick_id > sub.sub_last_tick
          and tick_queue = sub.sub_queue
        order by tick_queue asc, tick_id asc
        limit 1;
    if not found then
        -- nothing to do
        return null;
    end if;

    -- get next batch
    batch_id := nextval('pgq.batch_id_seq');
    update pgq.subscription
        set sub_batch = batch_id,
            sub_next_tick = next_tick,
            sub_active = now()
        where sub_queue = sub.sub_queue
          and sub_consumer = sub.sub_consumer;
    return batch_id;
end;
$$ language plpgsql security definer;


