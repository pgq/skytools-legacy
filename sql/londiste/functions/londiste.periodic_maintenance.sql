
create or replace function londiste.periodic_maintenance()
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.periodic_maintenance(0)
--
--      Clean random stuff.
-- ----------------------------------------------------------------------
begin

    -- clean old EXECUTE entries
    delete from londiste.applied_execute
        where execute_time < now() - '3 months'::interval;

    return 0;
end;
$$ language plpgsql; -- need admin access

