
create or replace function pgq_ext.version()
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.version(0)
--
--      Returns version string for pgq_ext.  ATM its SkyTools version
--      with suffix that is only bumped when pgq_ext database code changes.
-- ----------------------------------------------------------------------
begin
    return '3.0.0.3';
end;
$$ language plpgsql;

