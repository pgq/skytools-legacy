
create or replace function pgq.unregister_consumer(
    x_queue_name text,
    x_consumer_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.unregister_consumer(2)
--
--      Unsubscriber consumer from the queue.  Also consumer's
--      retry events are deleted.
--
-- Parameters:
--      x_queue_name        - Name of the queue
--      x_consumer_name     - Name of the consumer
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    x_sub_id integer;
    _sub_id_cnt integer;
begin
    select s.sub_id into x_sub_id
        from pgq.subscription s, pgq.consumer c, pgq.queue q
        where s.sub_queue = q.queue_id
          and s.sub_consumer = c.co_id
          and q.queue_name = x_queue_name
          and c.co_name = x_consumer_name
        for update of s;
    if not found then
        return 0;
    end if;

    -- consumer + subconsumer count
    select count(*) into _sub_id_cnt
        from pgq.subscription
       where sub_id = x_sub_id;

    -- retry events
    delete from pgq.retry_queue
        where ev_owner = x_sub_id;

    -- this will drop subconsumers too
    delete from pgq.subscription
        where sub_id = x_sub_id;

    return _sub_id_cnt;
end;
$$ language plpgsql security definer;

