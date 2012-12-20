
create or replace function londiste.version()
returns text as $$
-- ----------------------------------------------------------------------
-- Function: londiste.version(0)
--
--      Returns version string for londiste.  ATM it is based on SkyTools version
--      and only bumped when database code changes.
-- ----------------------------------------------------------------------
begin
    return '3.1.3';
end;
$$ language plpgsql;

