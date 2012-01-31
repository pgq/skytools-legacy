
create or replace function pgq_ext.set_last_tick(
    a_consumer text,
    a_subconsumer text,
    a_tick_id bigint)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.set_last_tick(3)
--
--	    records last completed tick for consumer,
--      removes row if a_tick_id is NULL 
--
-- Parameters:
--      a_consumer - consumer name
--      a_subconsumer - subconsumer name
--      a_tick_id - a tick id
--
-- Returns:
--      1
-- Calls:
--      None
-- Tables directly manipulated:
--      delete - pgq_ext.completed_tick
--      update - pgq_ext.completed_tick
--      insert - pgq_ext.completed_tick 
-- ----------------------------------------------------------------------
begin
    if a_tick_id is null then
        delete from pgq_ext.completed_tick
         where consumer_id = a_consumer
           and subconsumer_id = a_subconsumer;
    else   
        update pgq_ext.completed_tick
           set last_tick_id = a_tick_id
         where consumer_id = a_consumer
           and subconsumer_id = a_subconsumer;
        if not found then
            insert into pgq_ext.completed_tick
                (consumer_id, subconsumer_id, last_tick_id)
                values (a_consumer, a_subconsumer, a_tick_id);
        end if;
    end if;

    return 1;
end;
$$ language plpgsql security definer;

create or replace function pgq_ext.set_last_tick(
    a_consumer text,
    a_tick_id bigint)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.set_last_tick(2)
--
--	    records last completed tick for consumer,
--      removes row if a_tick_id is NULL 
--
-- Parameters:
--      a_consumer - consumer name
--      a_tick_id - a tick id
--
-- Returns:
--      1
-- Calls:
--      pgq_ext.set_last_tick(2)
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
begin
    return pgq_ext.set_last_tick(a_consumer, '', a_tick_id);
end;
$$ language plpgsql;

