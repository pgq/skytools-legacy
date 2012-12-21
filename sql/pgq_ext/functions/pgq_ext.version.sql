
create or replace function pgq_ext.version()
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq_ext.version(0)
--
--      Returns version string for pgq_ext.  ATM it is based SkyTools version
--      only bumped when database code changes.
-- ----------------------------------------------------------------------
begin
    return '3.1';
end;
$$ language plpgsql;

