
create or replace function pgq_ext.set_batch_done(
    a_consumer text,
    a_subconsumer text,
    a_batch_id bigint)
returns boolean as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.set_batch_done(3)
--
--	    Marks a batch as "done"  for certain consumer and subconsumer 
--
-- Parameters:
--      a_consumer - consumer name
--      a_subconsumer - subconsumer name
--      a_batch_id - a batch id
--
-- Returns:
--      false if it already was done
--	    true for successfully marking it as done 
-- Calls:
--      None
-- Tables directly manipulated:
--      update - pgq_ext.completed_batch
-- ----------------------------------------------------------------------
begin
    if pgq_ext.is_batch_done(a_consumer, a_subconsumer, a_batch_id) then
        return false;
    end if;

    if a_batch_id > 0 then
        update pgq_ext.completed_batch
           set last_batch_id = a_batch_id
         where consumer_id = a_consumer
           and subconsumer_id = a_subconsumer;
        if not found then
            insert into pgq_ext.completed_batch (consumer_id, subconsumer_id, last_batch_id)
                values (a_consumer, a_subconsumer, a_batch_id);
        end if;
    end if;

    return true;
end;
$$ language plpgsql security definer;

create or replace function pgq_ext.set_batch_done(
    a_consumer text,
    a_batch_id bigint)
returns boolean as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.set_batch_done(3)
--
--	    Marks a batch as "done"  for certain consumer 
--
-- Parameters:
--      a_consumer - consumer name
--      a_batch_id - a batch id
--
-- Returns:
--      false if it already was done
--	    true for successfully marking it as done 
-- Calls:
--      pgq_ext.set_batch_done(3)
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
begin
    return pgq_ext.set_batch_done(a_consumer, '', a_batch_id);
end;
$$ language plpgsql;

