-- ----------------------------------------------------------------------
-- Function: pgq.version(0)
--
--      Returns verison string for pgq.
-- ----------------------------------------------------------------------
create or replace function pgq.version()
returns text as $$
begin
    return '2.1.4';
end;
$$ language plpgsql;

