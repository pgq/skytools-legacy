
create or replace function pgq_ext.is_event_done(
    a_consumer text,
    a_subconsumer text,
    a_batch_id bigint,
    a_event_id bigint)
returns boolean as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.is_event_done(4)
--
--	    Checks if a certain consumer and subconsumer have "done" and event
--      in a batch  
--
-- Parameters:
--      a_consumer - consumer name
--      a_subconsumer - subconsumer name
--      a_batch_id - a batch id
--      a_event_id - an event id
--
-- Returns:
--	    true if event is done, else false 
-- Calls:
--      None
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
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
-- ----------------------------------------------------------------------
-- Function: pgq_ext.is_event_done(3)
--
--	    Checks if a certain consumer has "done" and event
--      in a batch  
--
-- Parameters:
--      a_consumer - consumer name
--      a_batch_id - a batch id
--      a_event_id - an event id
--
-- Returns:
--	    true if event is done, else false 
-- Calls:
--      Nonpgq_ext.is_event_done(4)
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
begin
    return pgq_ext.is_event_done(a_consumer, '', a_batch_id, a_event_id);
end;
$$ language plpgsql;

