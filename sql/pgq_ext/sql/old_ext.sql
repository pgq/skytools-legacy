


set client_min_messages = 'warning';
set default_with_oids = 'off';

create schema pgq_ext;
grant usage on schema pgq_ext to public;


--
-- batch tracking
--
create table pgq_ext.completed_batch (
    consumer_id   text not null,
    last_batch_id bigint not null,

    primary key (consumer_id)
);


--
-- event tracking
--
create table pgq_ext.completed_event (
    consumer_id   text not null,
    batch_id      bigint not null,
    event_id      bigint not null,

    primary key (consumer_id, batch_id, event_id)
);

create table pgq_ext.partial_batch (
    consumer_id   text not null,
    cur_batch_id  bigint not null,

    primary key (consumer_id)
);

--
-- tick tracking for SerialConsumer()
-- no access functions provided here
--
create table pgq_ext.completed_tick (
    consumer_id   text not null,
    last_tick_id  bigint not null,

    primary key (consumer_id)
);





create or replace function pgq_ext.is_batch_done(
    a_consumer text, a_batch_id bigint)
returns boolean as $$
declare
    res   boolean;
begin
    select last_batch_id = a_batch_id
      into res from pgq_ext.completed_batch
     where consumer_id = a_consumer;
    if not found then
        return false;
    end if;
    return res;
end;
$$ language plpgsql security definer;

create or replace function pgq_ext.set_batch_done(
    a_consumer text, a_batch_id bigint)
returns boolean as $$
begin
    if pgq_ext.is_batch_done(a_consumer, a_batch_id) then
        return false;
    end if;

    if a_batch_id > 0 then
        update pgq_ext.completed_batch
           set last_batch_id = a_batch_id
         where consumer_id = a_consumer;
        if not found then
            insert into pgq_ext.completed_batch (consumer_id, last_batch_id)
                values (a_consumer, a_batch_id);
        end if;
    end if;

    return true;
end;
$$ language plpgsql security definer;




create or replace function pgq_ext.is_event_done(
    a_consumer text,
    a_batch_id bigint, a_event_id bigint)
returns boolean as $$
declare
    res   bigint;
begin
    perform 1 from pgq_ext.completed_event
     where consumer_id = a_consumer
       and batch_id = a_batch_id
       and event_id = a_event_id;
    return found;
end;
$$ language plpgsql security definer;

create or replace function pgq_ext.set_event_done(
    a_consumer text, a_batch_id bigint, a_event_id bigint)
returns boolean as $$
declare
    old_batch bigint;
begin
    -- check if done
    perform 1 from pgq_ext.completed_event
     where consumer_id = a_consumer
       and batch_id = a_batch_id
       and event_id = a_event_id;
    if found then
        return false;
    end if;

    -- if batch changed, do cleanup
    select cur_batch_id into old_batch
        from pgq_ext.partial_batch
        where consumer_id = a_consumer;
    if not found then
        -- first time here
        insert into pgq_ext.partial_batch
            (consumer_id, cur_batch_id)
            values (a_consumer, a_batch_id);
    elsif old_batch <> a_batch_id then
        -- batch changed, that means old is finished on queue db
        -- thus the tagged events are not needed anymore
        delete from pgq_ext.completed_event
            where consumer_id = a_consumer
              and batch_id = old_batch;
        -- remember current one
        update pgq_ext.partial_batch
            set cur_batch_id = a_batch_id
            where consumer_id = a_consumer;
    end if;

    -- tag as done
    insert into pgq_ext.completed_event (consumer_id, batch_id, event_id)
      values (a_consumer, a_batch_id, a_event_id);

    return true;
end;
$$ language plpgsql security definer;




create or replace function pgq_ext.get_last_tick(a_consumer text)
returns int8 as $$
declare
    res   int8;
begin
    select last_tick_id into res
      from pgq_ext.completed_tick
     where consumer_id = a_consumer;
    return res;
end;
$$ language plpgsql security definer;

create or replace function pgq_ext.set_last_tick(a_consumer text, a_tick_id bigint)
returns integer as $$
begin
    if a_tick_id is null then
        delete from pgq_ext.completed_tick
         where consumer_id = a_consumer;
    else   
        update pgq_ext.completed_tick
           set last_tick_id = a_tick_id
         where consumer_id = a_consumer;
        if not found then
            insert into pgq_ext.completed_tick (consumer_id, last_tick_id)
                values (a_consumer, a_tick_id);
        end if;
    end if;

    return 1;
end;
$$ language plpgsql security definer;




create or replace function pgq_ext.version()
returns text as $$
begin
    return '3.0.0.1';
end;
$$ language plpgsql;






