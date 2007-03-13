
create or replace function pgq.failed_event_list(
    x_queue_name text,
    x_consumer_name text)
returns setof pgq.failed_queue as $$ 
-- ----------------------------------------------------------------------
-- Function: pgq.failed_event_list(2)
--
--      Get list of all failed events for one consumer.
--
-- Parameters:
--      x_queue_name        - Queue name
--      x_consumer_name     - Consumer name
--
-- Returns:
--      List of failed events.
-- ----------------------------------------------------------------------
declare 
    rec pgq.failed_queue%rowtype; 
begin 
    for rec in
        select fq.*
          from pgq.failed_queue fq, pgq.consumer,
               pgq.queue, pgq.subscription
         where queue_name = x_queue_name
           and co_name = x_consumer_name
           and sub_consumer = co_id
           and sub_queue = queue_id
           and ev_owner = sub_id
        order by ev_id
    loop
        return next rec; 
    end loop; 
    return;
end; 
$$ language plpgsql security definer;

create or replace function pgq.failed_event_list(
    x_queue_name text,
    x_consumer_name text,
    x_count integer,
    x_offset integer)
returns setof pgq.failed_queue as $$ 
-- ----------------------------------------------------------------------
-- Function: pgq.failed_event_list(4)
--
--      Get list of failed events, from offset and specific count.
--
-- Parameters:
--      x_queue_name        - Queue name
--      x_consumer_name     - Consumer name
--      x_count             - Max amount of events to fetch
--      x_offset            - From this offset
--
-- Returns:
--      List of failed events.
-- ----------------------------------------------------------------------
declare 
    rec pgq.failed_queue%rowtype; 
begin 
    for rec in
        select fq.*
          from pgq.failed_queue fq, pgq.consumer,
               pgq.queue, pgq.subscription
         where queue_name = x_queue_name
           and co_name = x_consumer_name
           and sub_consumer = co_id
           and sub_queue = queue_id
           and ev_owner = sub_id
        order by ev_id
        limit x_count
        offset x_offset
    loop
        return next rec; 
    end loop; 
    return;
end; 
$$ language plpgsql security definer;

create or replace function pgq.failed_event_count(
    x_queue_name text,
    x_consumer_name text)
returns integer as $$ 
-- ----------------------------------------------------------------------
-- Function: pgq.failed_event_count(2)
--
--      Get size of failed event queue.
--
-- Parameters:
--      x_queue_name        - Queue name
--      x_consumer_name     - Consumer name
--
-- Returns:
--      Number of failed events in failed event queue.
-- ----------------------------------------------------------------------
declare 
    ret integer;
begin 
    select count(1) into ret
      from pgq.failed_queue, pgq.consumer, pgq.queue, pgq.subscription
     where queue_name = x_queue_name
       and co_name = x_consumer_name
       and sub_queue = queue_id
       and sub_consumer = co_id
       and ev_owner = sub_id;
    return ret;
end; 
$$ language plpgsql security definer;

create or replace function pgq.failed_event_delete(
    x_queue_name text,
    x_consumer_name text,
    x_event_id bigint)
returns integer as $$ 
-- ----------------------------------------------------------------------
-- Function: pgq.failed_event_delete(3)
--
--      Delete specific event from failed event queue.
--
-- Parameters:
--      x_queue_name        - Queue name
--      x_consumer_name     - Consumer name
--      x_event_id          - Event ID
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare 
    x_sub_id integer;
begin 
    select sub_id into x_sub_id
      from pgq.subscription, pgq.consumer, pgq.queue
     where queue_name = x_queue_name
       and co_name = x_consumer_name
       and sub_consumer = co_id
       and sub_queue = queue_id;
    if not found then
        raise exception 'no such queue/consumer';
    end if;

    delete from pgq.failed_queue
     where ev_owner = x_sub_id
       and ev_id = x_event_id;
    if not found then
        raise exception 'event not found';
    end if;

    return 1;
end; 
$$ language plpgsql security definer;

create or replace function pgq.failed_event_retry(
    x_queue_name text,
    x_consumer_name text,
    x_event_id bigint)
returns bigint as $$ 
-- ----------------------------------------------------------------------
-- Function: pgq.failed_event_retry(3)
--
--      Insert specific event from failed queue to main queue.
--
-- Parameters:
--      x_queue_name        - Queue name
--      x_consumer_name     - Consumer name
--      x_event_id          - Event ID
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare 
    ret         bigint;
    x_sub_id    integer;
begin 
    select sub_id into x_sub_id
      from pgq.subscription, pgq.consumer, pgq.queue
     where queue_name = x_queue_name
       and co_name = x_consumer_name
       and sub_consumer = co_id
       and sub_queue = queue_id;
    if not found then
        raise exception 'no such queue/consumer';
    end if;

    select pgq.insert_event_raw(x_queue_name, ev_id, ev_time,
            ev_owner, ev_retry, ev_type, ev_data,
            ev_extra1, ev_extra2, ev_extra3, ev_extra4)
      into ret
      from pgq.failed_queue, pgq.consumer, pgq.queue
     where ev_owner = x_sub_id
       and ev_id = x_event_id;
    if not found then
        raise exception 'event not found';
    end if;

    perform pgq.failed_event_delete(x_queue_name, x_consumer_name, x_event_id);

    return ret;
end; 
$$ language plpgsql security definer;


