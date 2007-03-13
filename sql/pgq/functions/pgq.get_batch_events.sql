create or replace function pgq.get_batch_events(x_batch_id bigint)
returns setof pgq.ret_batch_event as $$ 
-- ----------------------------------------------------------------------
-- Function: pgq.get_batch_events(1)
--
--      Get all events in batch.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--
-- Returns:
--      List of events.
-- ----------------------------------------------------------------------
declare 
    rec pgq.ret_batch_event%rowtype; 
    sql text; 
begin 
    sql := pgq.batch_event_sql(x_batch_id); 
    for rec in execute sql loop
        return next rec; 
    end loop; 
    return;
end; 
$$ language plpgsql; -- no perms needed


