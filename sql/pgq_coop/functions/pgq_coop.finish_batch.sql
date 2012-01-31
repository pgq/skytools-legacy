create or replace function pgq_coop.finish_batch(
    i_batch_id bigint)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq_coop.finish_batch(1)
--
--	Closes a batch.
--
-- Parameters:
--	i_batch_id	- id of the batch to be closed
--
-- Returns:
--	1 if success (batch was found), 0 otherwise
-- Calls:
--      None
-- Tables directly manipulated:
--      update - pgq.subscription
-- ----------------------------------------------------------------------
begin
    -- we are dealing with subconsumer, so nullify all tick info
    -- tick columns for master consumer contain adequate data
    update pgq.subscription
       set sub_active = now(),
           sub_last_tick = null,
           sub_next_tick = null,
           sub_batch = null
     where sub_batch = i_batch_id;
    if not found then
        raise warning 'coop_finish_batch: batch % not found', i_batch_id;
        return 0;
    else
        return 1;
    end if;
end;
$$ language plpgsql security definer;

