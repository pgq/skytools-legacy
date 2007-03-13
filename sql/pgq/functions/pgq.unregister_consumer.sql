
create or replace function pgq.unregister_consumer(
    x_queue_name text,
    x_consumer_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.unregister_consumer(2)
--
--      Unsubscriber consumer from the queue.  Also consumer's failed
--      and retry events are deleted.
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
begin
    select sub_id into x_sub_id
        from pgq.subscription, pgq.consumer, pgq.queue
        where sub_queue = queue_id
          and sub_consumer = co_id
          and queue_name = x_queue_name
          and co_name = x_consumer_name;
    if not found then
        raise exception 'consumer not registered on queue';
    end if;

    delete from pgq.retry_queue
        where ev_owner = x_sub_id;

    delete from pgq.failed_queue
        where ev_owner = x_sub_id;

    delete from pgq.subscription
        where sub_id = x_sub_id;

    return 1;
end;
$$ language plpgsql security definer;

