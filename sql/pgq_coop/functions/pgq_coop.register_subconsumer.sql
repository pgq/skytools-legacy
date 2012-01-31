create or replace function pgq_coop.register_subconsumer(
    i_queue_name text,
    i_consumer_name text,
    i_subconsumer_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq_coop.register_subconsumer(3)
--
--	    Subscribe a subconsumer on a queue.
--
--      Subconsumer will be registered as another consumer on queue,
--      whose name will be i_consumer_name and i_subconsumer_name
--      combined.
--
-- Returns:
--	    0 - if already registered
--	    1 - if this is a new registration
--
-- Calls:
--      pgq.register_consumer(i_queue_name, i_consumer_name)
--      pgq.register_consumer(i_queue_name, _subcon_name);
--
-- Tables directly manipulated:
--      update - pgq.subscription
-- 
-- ----------------------------------------------------------------------
declare
    _subcon_name text; -- consumer + subconsumer
    _queue_id integer;
    _consumer_id integer;
    _subcon_id integer;
    _consumer_sub_id integer;
    _subcon_result integer;
    r record;
begin
    _subcon_name := i_consumer_name || '.' || i_subconsumer_name;

    -- make sure main consumer exists
    perform pgq.register_consumer(i_queue_name, i_consumer_name);

    -- just go and register the subconsumer as a regular consumer
    _subcon_result := pgq.register_consumer(i_queue_name, _subcon_name);

    -- if it is a new registration
    if _subcon_result = 1 then
        select q.queue_id, mainc.co_id as main_consumer_id,
               s.sub_id as main_consumer_sub_id,
               subc.co_id as sub_consumer_id
            into r
            from pgq.queue q, pgq.subscription s, pgq.consumer mainc, pgq.consumer subc
            where mainc.co_name = i_consumer_name
              and subc.co_name = _subcon_name
              and q.queue_name = i_queue_name
              and s.sub_queue = q.queue_id
              and s.sub_consumer = mainc.co_id;
        if not found then
            raise exception 'main consumer not found';
        end if;

        -- duplicate the sub_id of consumer to the subconsumer
        update pgq.subscription s
            set sub_id = r.main_consumer_sub_id,
                sub_last_tick = null,
                sub_next_tick = null
            where sub_queue = r.queue_id
              and sub_consumer = r.sub_consumer_id;
    end if;

    return _subcon_result;
end;
$$ language plpgsql security definer;

