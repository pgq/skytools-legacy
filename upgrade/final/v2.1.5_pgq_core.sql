
begin;

alter table pgq.subscription add constraint subscription_ukey unique (sub_queue, sub_consumer);
create index rq_retry_owner_idx on pgq.retry_queue (ev_owner, ev_id);


create or replace function pgq.current_event_table(x_queue_name text)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.current_event_table(1)
--
--      Return active event table for particular queue.
--      Event can be added to it without going via functions,
--      e.g. by COPY.
--
-- Note:
--      The result is valid only during current transaction.
--
-- Permissions:
--      Actual insertion requires superuser access.
--
-- Parameters:
--      x_queue_name    - Queue name.
-- ----------------------------------------------------------------------
declare
    res text;
begin
    select queue_data_pfx || '_' || queue_cur_table into res
        from pgq.queue where queue_name = x_queue_name;
    if not found then
        raise exception 'Event queue not found';
    end if;
    return res;
end;
$$ language plpgsql; -- no perms needed



create or replace function pgq.event_failed(
    x_batch_id bigint,
    x_event_id bigint,
    x_reason text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.event_failed(3)
--
--      Copies the event to failed queue so it can be looked at later.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--      x_event_id      - Event id
--      x_reason        - Text to associate with event.
--
-- Returns:
--     0 if event was already in queue, 1 otherwise.
-- ----------------------------------------------------------------------
begin
    insert into pgq.failed_queue (ev_failed_reason, ev_failed_time,
        ev_id, ev_time, ev_txid, ev_owner, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    select x_reason, now(),
           ev_id, ev_time, NULL, sub_id, coalesce(ev_retry, 0),
           ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4
      from pgq.get_batch_events(x_batch_id),
           pgq.subscription
     where sub_batch = x_batch_id
       and ev_id = x_event_id;
    if not found then
        raise exception 'event not found';
    end if;
    return 1;

-- dont worry if the event is already in queue
exception
    when unique_violation then
        return 0;
end;
$$ language plpgsql security definer;



create or replace function pgq.event_retry(
    x_batch_id bigint,
    x_event_id bigint,
    x_retry_time timestamptz)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.event_retry(3)
--
--     Put the event into retry queue, to be processed again later.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--      x_event_id      - event id
--      x_retry_time    - Time when the event should be put back into queue
--
-- Returns:
--     nothing
-- ----------------------------------------------------------------------
begin
    insert into pgq.retry_queue (ev_retry_after,
        ev_id, ev_time, ev_txid, ev_owner, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    select x_retry_time,
           ev_id, ev_time, NULL, sub_id, coalesce(ev_retry, 0) + 1,
           ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4
      from pgq.get_batch_events(x_batch_id),
           pgq.subscription
     where sub_batch = x_batch_id
       and ev_id = x_event_id;
    if not found then
        raise exception 'event not found';
    end if;
    return 1;

-- dont worry if the event is already in queue
exception
    when unique_violation then
        return 0;
end;
$$ language plpgsql security definer;


create or replace function pgq.event_retry(
    x_batch_id bigint,
    x_event_id bigint,
    x_retry_seconds integer)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.event_retry(3)
--
--     Put the event into retry queue, to be processed later again.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--      x_event_id      - event id
--      x_retry_seconds - Time when the event should be put back into queue
--
-- Returns:
--     nothing
-- ----------------------------------------------------------------------
declare
    new_retry  timestamptz;
begin
    new_retry := current_timestamp + ((x_retry_seconds || ' seconds')::interval);
    return pgq.event_retry(x_batch_id, x_event_id, new_retry);
end;
$$ language plpgsql security definer;





create or replace function pgq.force_tick(i_queue_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.force_tick(2)
--
--      Simulate lots of events happening to force ticker to tick.
--
--      Should be called in loop, with some delay until last tick
--      changes or too much time is passed.
--
--      Such function is needed because paraller calls of pgq.ticker() are
--      dangerous, and cannot be protected with locks as snapshot
--      is taken before locking.
--
-- Parameters:
--      i_queue_name     - Name of the queue
--
-- Returns:
--      Currently last tick id.
-- ----------------------------------------------------------------------
declare
    q  record;
    t  record;
begin
    -- bump seq and get queue id
    select queue_id,
           setval(queue_event_seq, nextval(queue_event_seq)
                                   + queue_ticker_max_count * 2) as tmp
      into q from pgq.queue
     where queue_name = i_queue_name
       and not queue_external_ticker;
    if not found then
        raise exception 'queue not found or ticks not allowed';
    end if;

    -- return last tick id
    select tick_id into t from pgq.tick
     where tick_queue = q.queue_id
     order by tick_queue desc, tick_id desc limit 1;

    return t.tick_id;
end;
$$ language plpgsql security definer;



create or replace function pgq.grant_perms(x_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.grant_perms(1)
--
--      Make event tables readable by public.
--
-- Parameters:
--      x_queue_name        - Name of the queue.
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    q           record;
    i           integer;
    tbl_perms   text;
    seq_perms   text;
begin
    select * from pgq.queue into q
        where queue_name = x_queue_name;
    if not found then
        raise exception 'Queue not found';
    end if;

    if true then
        -- safe, all access must go via functions
        seq_perms := 'select';
        tbl_perms := 'select';
    else
        -- allow ordinery users to directly insert
        -- to event tables.  dangerous.
        seq_perms := 'select, update';
        tbl_perms := 'select, insert';
    end if;

    -- tick seq, normal users don't need to modify it
    execute 'grant ' || seq_perms
        || ' on ' || q.queue_tick_seq || ' to public';

    -- event seq
    execute 'grant ' || seq_perms
        || ' on ' || q.queue_event_seq || ' to public';
    
    -- parent table for events
    execute 'grant select on ' || q.queue_data_pfx || ' to public';

    -- real event tables
    for i in 0 .. q.queue_ntables - 1 loop
        execute 'grant ' || tbl_perms
            || ' on ' || q.queue_data_pfx || '_' || i
            || ' to public';
    end loop;

    return 1;
end;
$$ language plpgsql security definer;



create or replace function pgq.insert_event(queue_name text, ev_type text, ev_data text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.insert_event(3)
--
--      Insert a event into queue.
--
-- Parameters:
--      queue_name      - Name of the queue
--      ev_type         - User-specified type for the event
--      ev_data         - User data for the event
--
-- Returns:
--      Event ID
-- ----------------------------------------------------------------------
begin
    return pgq.insert_event(queue_name, ev_type, ev_data, null, null, null, null);
end;
$$ language plpgsql security definer;



create or replace function pgq.insert_event(
    queue_name text, ev_type text, ev_data text,
    ev_extra1 text, ev_extra2 text, ev_extra3 text, ev_extra4 text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq.insert_event(7)
--
--      Insert a event into queue with all the extra fields.
--
-- Parameters:
--      queue_name      - Name of the queue
--      ev_type         - User-specified type for the event
--      ev_data         - User data for the event
--      ev_extra1       - Extra data field for the event
--      ev_extra2       - Extra data field for the event
--      ev_extra3       - Extra data field for the event
--      ev_extra4       - Extra data field for the event
--
-- Returns:
--      Event ID
-- ----------------------------------------------------------------------
begin
    return pgq.insert_event_raw(queue_name, null, now(), null, null,
            ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4);
end;
$$ language plpgsql security definer;



create or replace function pgq.maint_tables_to_vacuum()
returns setof text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.maint_tables_to_vacuum(0)
--
--      Returns list of tablenames that need frequent vacuuming.
--
--      The goal is to avoid hardcoding them into maintenance process.
--
-- Returns:
--      List of table names.
-- ----------------------------------------------------------------------
declare
    row record;
begin
    return next 'pgq.subscription';
    return next 'pgq.consumer';
    return next 'pgq.queue';
    return next 'pgq.tick';
    return next 'pgq.retry_queue';

    -- include also txid, pgq_ext and londiste tables if they exist
    for row in
        select n.nspname as scm, t.relname as tbl
          from pg_class t, pg_namespace n
         where n.oid = t.relnamespace
           and n.nspname = 'txid' and t.relname = 'epoch'
        union all
        select n.nspname as scm, t.relname as tbl
          from pg_class t, pg_namespace n
         where n.oid = t.relnamespace
           and n.nspname = 'londiste' and t.relname = 'completed'
        union all
        select n.nspname as scm, t.relname as tbl
          from pg_class t, pg_namespace n
         where n.oid = t.relnamespace
           and n.nspname = 'pgq_ext'
           and t.relname in ('completed_tick', 'completed_batch', 'completed_event', 'partial_batch')
    loop
        return next row.scm || '.' || row.tbl;
    end loop;

    return;
end;
$$ language plpgsql;




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




create or replace function pgq.register_consumer(
    x_queue_name text,
    x_consumer_id text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.register_consumer(2)
--
--      Subscribe consumer on a queue.
--
--      From this moment forward, consumer will see all events in the queue.
--
-- Parameters:
--      x_queue_name        - Name of queue
--      x_consumer_name     - Name of consumer
--
-- Returns:
--      0  - if already registered
--      1  - if new registration
-- ----------------------------------------------------------------------
begin
    return pgq.register_consumer(x_queue_name, x_consumer_id, NULL);
end;
$$ language plpgsql security definer;


create or replace function pgq.register_consumer(
    x_queue_name text,
    x_consumer_name text,
    x_tick_pos bigint)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.register_consumer(3)
--
--      Extended registration, allows to specify tick_id.
--
-- Note:
--      For usage in special situations.
--
-- Parameters:
--      x_queue_name        - Name of a queue
--      x_consumer_name     - Name of consumer
--      x_tick_pos          - Tick ID
--
-- Returns:
--      0/1 whether consumer has already registered.
-- ----------------------------------------------------------------------
declare
    tmp         text;
    last_tick   bigint;
    x_queue_id          integer;
    x_consumer_id integer;
    queue integer;
    sub record;
begin
    select queue_id into x_queue_id from pgq.queue
        where queue_name = x_queue_name;
    if not found then
        raise exception 'Event queue not created yet';
    end if;

    -- get consumer and create if new
    select co_id into x_consumer_id from pgq.consumer
        where co_name = x_consumer_name;
    if not found then
        insert into pgq.consumer (co_name) values (x_consumer_name);
        x_consumer_id := currval('pgq.consumer_co_id_seq');
    end if;

    -- if particular tick was requested, check if it exists
    if x_tick_pos is not null then
        perform 1 from pgq.tick
            where tick_queue = x_queue_id
              and tick_id = x_tick_pos;
        if not found then
            raise exception 'cannot reposition, tick not found: %', x_tick_pos;
        end if;
    end if;

    -- check if already registered
    select sub_last_tick, sub_batch into sub
        from pgq.subscription
        where sub_consumer = x_consumer_id
          and sub_queue  = x_queue_id;
    if found then
        if x_tick_pos is not null then
            if sub.sub_batch is not null then
                raise exception 'reposition while active not allowed';
            end if;
            -- update tick pos if requested
            update pgq.subscription
                set sub_last_tick = x_tick_pos
                where sub_consumer = x_consumer_id
                  and sub_queue = x_queue_id;
        end if;
        -- already registered
        return 0;
    end if;

    --  new registration
    if x_tick_pos is null then
        -- start from current tick
        select tick_id into last_tick from pgq.tick
            where tick_queue = x_queue_id
            order by tick_queue desc, tick_id desc
            limit 1;
        if not found then
            raise exception 'No ticks for this queue.  Please run ticker on database.';
        end if;
    else
        last_tick := x_tick_pos;
    end if;

    -- register
    insert into pgq.subscription (sub_queue, sub_consumer, sub_last_tick)
        values (x_queue_id, x_consumer_id, last_tick);
    return 1;
end;
$$ language plpgsql security definer;




create or replace function pgq.version()
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.version(0)
--
--      Returns verison string for pgq.  ATM its SkyTools version
--      that is only bumped when PGQ database code changes.
-- ----------------------------------------------------------------------
begin
    return '2.1.5';
end;
$$ language plpgsql;




grant usage on schema pgq to public;
grant select on table pgq.consumer to public;
grant select on table pgq.queue to public;
grant select on table pgq.tick to public;
grant select on table pgq.queue to public;
grant select on table pgq.subscription to public;
grant select on table pgq.event_template to public;
grant select on table pgq.retry_queue to public;
grant select on table pgq.failed_queue to public;


end;


