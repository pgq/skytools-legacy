create or replace function pgq_coop.unregister_subconsumer(
    i_queue_name text,
    i_consumer_name text,
    i_subconsumer_name text,
    i_batch_handling integer)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq_coop.unregister_subconsumer(4)
--
--      Unregisters subconsumer from the queue.
--
--      If consumer has active batch, then behviour depends on
--      i_batch_handling parameter.
--
-- Values for i_batch_handling:
--      0 - Fail with an exception.
--      1 - Close the batch, ignoring the events.
--
-- Returns:
--	    0 - no consumer found
--      1 - consumer found and unregistered
--
-- Tables directly manipulated:
--      delete - pgq.subscription
--
-- ----------------------------------------------------------------------
declare
    _current_batch bigint;
    _queue_id integer;
    _subcon_id integer;
begin
    select q.queue_id, c.co_id, sub_batch
        into _queue_id, _subcon_id, _current_batch
        from pgq.queue q, pgq.consumer c, pgq.subscription s
        where c.co_name = i_consumer_name || '.' || i_subconsumer_name
          and q.queue_name = i_queue_name
          and s.sub_queue = q.queue_id
          and s.sub_consumer = c.co_id;
    if not found then
        return 0;
    end if;

    if _current_batch is not null then
        if i_batch_handling = 1 then
            -- ignore active batch
        else
            raise exception 'subconsumer has active batch';
        end if;
    end if;

    delete from pgq.subscription
        where sub_queue = _queue_id
          and sub_consumer = _subcon_id;

    return 1;
end;
$$ language plpgsql security definer;

