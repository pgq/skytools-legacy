
create or replace function pgq_coop.version()
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq_coop.version(0)
--
--      Returns version string for pgq_coop.  ATM its SkyTools version
--      with suffix that is only bumped when pgq_coop database code changes.
-- ----------------------------------------------------------------------
begin
    return '3.0.0.3';
end;
$$ language plpgsql;

