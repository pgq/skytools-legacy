create or replace function pgq.maint_tables_to_vacuum()
returns setof text as $$
-- ----------------------------------------------------------------------
-- Function: pgq.maint_tables_to_vacuum(0)
--
--      Returns list of tablenames that need frequent vacuuming.
--
--      The goal is to avoid hardcoding them into maintenance process.
--
-- Returns:
--      List of table names.
-- ----------------------------------------------------------------------
declare
    row record;
begin
    return next 'pgq.subscription';
    return next 'pgq.consumer';
    return next 'pgq.queue';
    return next 'pgq.tick';
    return next 'pgq.retry_queue';

    -- include also txid, pgq_ext and londiste tables if they exist
    for row in
        select n.nspname as scm, t.relname as tbl
          from pg_class t, pg_namespace n
         where n.oid = t.relnamespace
           and n.nspname = 'txid' and t.relname = 'epoch'
        union all
        select n.nspname as scm, t.relname as tbl
          from pg_class t, pg_namespace n
         where n.oid = t.relnamespace
           and n.nspname = 'londiste' and t.relname = 'completed'
        union all
        select n.nspname as scm, t.relname as tbl
          from pg_class t, pg_namespace n
         where n.oid = t.relnamespace
           and n.nspname = 'pgq_ext'
           and t.relname in ('completed_tick', 'completed_batch', 'completed_event', 'partial_batch')
    loop
        return next row.scm || '.' || row.tbl;
    end loop;

    return;
end;
$$ language plpgsql;


