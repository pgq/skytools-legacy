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
begin
    return next 'pgq.subscription';
    return next 'pgq.consumer';
    return next 'pgq.queue';
    return next 'pgq.tick';
    return next 'pgq.retry_queue';

    -- vacuum also txid.epoch, if exists
    perform 1 from pg_class t, pg_namespace n
        where t.relname = 'epoch'
          and n.nspname = 'txid'
          and n.oid = t.relnamespace;
    if found then
        return next 'txid.epoch';
    end if;

    return;
end;
$$ language plpgsql;


