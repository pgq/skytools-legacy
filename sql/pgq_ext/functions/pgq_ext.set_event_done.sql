
create or replace function pgq_ext.set_event_done(
    a_consumer text,
    a_subconsumer text,
    a_batch_id bigint,
    a_event_id bigint)
returns boolean as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.set_event_done(4)
--
--	    Marks and event done in a batch for a certain consumer and subconsumer
--
-- Parameters:
--      a_consumer - consumer name
--      a_subconsumer - subconsumer name
--      a_batch_id - a batch id
--      a_event_id - an event id
--
-- Returns:
--      false if already done
--	    true on success 
-- Calls:
--      None
-- Tables directly manipulated:
--      insert - pgq_ext.partial_batch
--      delete - pgq_ext.completed_event
--      update - pgq_ext.partial_batch
--      insert - pgq_ext.completed_event
-- ----------------------------------------------------------------------
declare
    old_batch bigint;
begin
    -- check if done
    perform 1 from pgq_ext.completed_event
     where consumer_id = a_consumer
       and subconsumer_id = a_subconsumer
       and batch_id = a_batch_id
       and event_id = a_event_id;
    if found then
        return false;
    end if;

    -- if batch changed, do cleanup
    select cur_batch_id into old_batch
        from pgq_ext.partial_batch
        where consumer_id = a_consumer
          and subconsumer_id = a_subconsumer;
    if not found then
        -- first time here
        insert into pgq_ext.partial_batch
            (consumer_id, subconsumer_id, cur_batch_id)
            values (a_consumer, a_subconsumer, a_batch_id);
    elsif old_batch <> a_batch_id then
        -- batch changed, that means old is finished on queue db
        -- thus the tagged events are not needed anymore
        delete from pgq_ext.completed_event
            where consumer_id = a_consumer
              and subconsumer_id = a_subconsumer
              and batch_id = old_batch;
        -- remember current one
        update pgq_ext.partial_batch
            set cur_batch_id = a_batch_id
            where consumer_id = a_consumer
              and subconsumer_id = a_subconsumer;
    end if;

    -- tag as done
    insert into pgq_ext.completed_event
        (consumer_id, subconsumer_id, batch_id, event_id)
        values (a_consumer, a_subconsumer, a_batch_id, a_event_id);

    return true;
end;
$$ language plpgsql security definer;

create or replace function pgq_ext.set_event_done(
    a_consumer text,
    a_batch_id bigint,
    a_event_id bigint)
returns boolean as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.set_event_done(3)
--
--	    Marks and event done in a batch for a certain consumer and subconsumer
--
-- Parameters:
--      a_consumer - consumer name
--      a_batch_id - a batch id
--      a_event_id - an event id
--
-- Returns:
--      false if already done
--	    true on success 
-- Calls:
--      pgq_ext.set_event_done(4)
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
begin
    return pgq_ext.set_event_done(a_consumer, '', a_batch_id, a_event_id);
end;
$$ language plpgsql;

