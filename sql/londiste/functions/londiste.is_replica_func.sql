
create or replace function londiste.is_replica_func(func_oid oid)
returns boolean as $$
-- ----------------------------------------------------------------------
-- Function: londiste.is_replica_func(1)
--
--      Returns true if function is a PgQ-based replication functions.
--      This also means it takes queue name as first argument.
-- ----------------------------------------------------------------------
select count(1) > 0
  from pg_proc f join pg_namespace n on (n.oid = f.pronamespace)
  where f.oid = $1 and n.nspname = 'pgq' and f.proname in ('sqltriga', 'logutriga');
$$ language sql strict stable;

