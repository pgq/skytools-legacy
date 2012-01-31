
create or replace function pgq_ext.is_batch_done(
    a_consumer text,
    a_subconsumer text,
    a_batch_id bigint)
returns boolean as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.is_batch_done(3)
--
--	    Checks if a certain consumer and subconsumer have completed the batch 
--
-- Parameters:
--      a_consumer - consumer name
--      a_subconsumer - subconsumer name
--      a_batch_id - a batch id
--
-- Returns:
--	    true if batch is done, else false 
-- Calls:
--      None
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
declare
    res   boolean;
begin
    select last_batch_id = a_batch_id
      into res from pgq_ext.completed_batch
     where consumer_id = a_consumer
       and subconsumer_id = a_subconsumer;
    if not found then
        return false;
    end if;
    return res;
end;
$$ language plpgsql security definer;

create or replace function pgq_ext.is_batch_done(
    a_consumer text,
    a_batch_id bigint)
returns boolean as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.is_batch_done(2)
--
--	    Checks if a certain consumer has completed the batch 
--
-- Parameters:
--      a_consumer - consumer name
--      a_batch_id - a batch id
--
-- Returns:
--	    true if batch is done, else false 
-- Calls:
--      pgq_ext.is_batch_done(3)
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
begin
    return pgq_ext.is_batch_done(a_consumer, '', a_batch_id);
end;
$$ language plpgsql;

