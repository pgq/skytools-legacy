
create or replace function pgq_ext.get_last_tick(a_consumer text, a_subconsumer text)
returns int8 as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.get_last_tick(2)
--
--	Gets last completed tick for this consumer 
--
-- Parameters:
--      a_consumer - consumer name
--      a_subconsumer - subconsumer name
--
-- Returns:
--	    tick_id - last completed tick 
-- Calls:
--      None
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
declare
    res   int8;
begin
    select last_tick_id into res
      from pgq_ext.completed_tick
     where consumer_id = a_consumer
       and subconsumer_id = a_subconsumer;
    return res;
end;
$$ language plpgsql security definer;

create or replace function pgq_ext.get_last_tick(a_consumer text)
returns int8 as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.get_last_tick(1)
--
--	Gets last completed tick for this consumer 
--
-- Parameters:
--      a_consumer - consumer name
--
-- Returns:
--	    tick_id - last completed tick 
-- Calls:
--      pgq_ext.get_last_tick(2)
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
begin
    return pgq_ext.get_last_tick(a_consumer, '');
end;
$$ language plpgsql;

