create or replace function pgq.drop_queue(x_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.drop_queue(1)
--
--     Drop queue and all associated tables.
--     No consumers must be listening on the queue.
--
-- ----------------------------------------------------------------------
declare
    tblname  text;
    q record;
    num integer;
begin
    -- check ares
    if x_queue_name is null then
        raise exception 'Invalid NULL value';
    end if;

    -- check if exists
    select * into q from pgq.queue
        where queue_name = x_queue_name;
    if not found then
        raise exception 'No such event queue';
    end if;

    -- check if no consumers
    select count(*) into num from pgq.subscription
        where sub_queue = q.queue_id;
    if num > 0 then
        raise exception 'cannot drop queue, consumers still attached';
    end if;

    -- drop data tables
    for i in 0 .. (q.queue_ntables - 1) loop
        tblname := q.queue_data_pfx || '_' || i;
        execute 'DROP TABLE ' || tblname;
    end loop;
    execute 'DROP TABLE ' || q.queue_data_pfx;

    -- delete ticks
    delete from pgq.tick where tick_queue = q.queue_id;

    -- drop seqs
    -- FIXME: any checks needed here?
    execute 'DROP SEQUENCE ' || q.queue_tick_seq;
    execute 'DROP SEQUENCE ' || q.queue_event_seq;

    -- delete event
    delete from pgq.queue
        where queue_name = x_queue_name;

    return 1;
end;
$$ language plpgsql security definer;

