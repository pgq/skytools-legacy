
create or replace function pgq_ext.is_event_done(
    a_consumer text,
    a_subconsumer text,
    a_batch_id bigint,
    a_event_id bigint)
returns boolean as $$
declare
    res   bigint;
begin
    perform 1 from pgq_ext.completed_event
     where consumer_id = a_consumer
       and subconsumer_id = a_subconsumer
       and batch_id = a_batch_id
       and event_id = a_event_id;
    return found;
end;
$$ language plpgsql security definer;

create or replace function pgq_ext.is_event_done(
    a_consumer text,
    a_batch_id bigint,
    a_event_id bigint)
returns boolean as $$
begin
    return pgq_ext.is_event_done(a_consumer, '', a_batch_id, a_event_id);
end;
$$ language plpgsql;

