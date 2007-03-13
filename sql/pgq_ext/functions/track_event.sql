
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

