
create or replace function pgq_node.version()
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.version(0)
--
--      Returns version string for pgq_node.  ATM it is based on SkyTools version
--      and only bumped when database code changes.
-- ----------------------------------------------------------------------
begin
    return '3.1.3';
end;
$$ language plpgsql;

