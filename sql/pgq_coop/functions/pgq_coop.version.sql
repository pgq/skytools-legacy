
create or replace function pgq_coop.version()
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq_coop.version(0)
--
--      Returns version string for pgq_coop.  ATM it is based on SkyTools version
--      and only bumped when database code changes.
-- ----------------------------------------------------------------------
begin
    return '3.1.1';
end;
$$ language plpgsql;

