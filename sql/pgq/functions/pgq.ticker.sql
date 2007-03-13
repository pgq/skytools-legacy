create or replace function pgq.ticker(i_queue_name text, i_tick_id bigint)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.ticker(2)
--
--     Insert a tick with a particular tick_id.
--
--     For external tickers.
--
-- Parameters:
--     i_queue_name     - Name of the queue
--     i_tick_id        - Id of new tick.
--
-- Returns:
--     Tick id.
-- ----------------------------------------------------------------------
begin
    insert into pgq.tick (tick_queue, tick_id)
    select queue_id, i_tick_id
        from pgq.queue
        where queue_name = i_queue_name
          and queue_external_ticker;
    if not found then
        raise exception 'queue not found';
    end if;
    return i_tick_id;
end;
$$ language plpgsql security definer; -- unsure about access

create or replace function pgq.ticker(i_queue_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.ticker(1)
--
--     Insert a tick with a tick_id from sequence.
--
--     For pgqadm usage.
--
-- Parameters:
--     i_queue_name     - Name of the queue
--
-- Returns:
--     Tick id.
-- ----------------------------------------------------------------------
declare
    res bigint;
    ext boolean;
    seq text;
    q record;
begin
    select queue_id, queue_tick_seq, queue_external_ticker into q
        from pgq.queue where queue_name = i_queue_name;
    if not found then
        raise exception 'no such queue';
    end if;

    if q.queue_external_ticker then
        raise exception 'This queue has external tick source.';
    end if;

    insert into pgq.tick (tick_queue, tick_id)
        values (q.queue_id, nextval(q.queue_tick_seq));

    res = currval(q.queue_tick_seq);
    return res;
end;
$$ language plpgsql security definer; -- unsure about access

create or replace function pgq.ticker() returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.ticker(0)
--
--     Creates ticks for all queues which dont have external ticker.
--
-- Returns:
--     Number of queues that were processed.
-- ----------------------------------------------------------------------
declare
    res bigint;
begin
    select count(pgq.ticker(queue_name)) into res 
        from pgq.queue where not queue_external_ticker;
    return res;
end;
$$ language plpgsql security definer;

