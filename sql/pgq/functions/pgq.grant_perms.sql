create or replace function pgq.grant_perms(x_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.grant_perms(1)
--
--      Make event tables readable by public.
--
-- Parameters:
--      x_queue_name        - Name of the queue.
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    q           record;
    i           integer;
    tbl_perms   text;
    seq_perms   text;
begin
    select * from pgq.queue into q
        where queue_name = x_queue_name;
    if not found then
        raise exception 'Queue not found';
    end if;

    if true then
        -- safe, all access must go via functions
        seq_perms := 'select';
        tbl_perms := 'select';
    else
        -- allow ordinery users to directly insert
        -- to event tables.  dangerous.
        seq_perms := 'select, update';
        tbl_perms := 'select, insert';
    end if;

    -- tick seq, normal users don't need to modify it
    execute 'grant ' || seq_perms
        || ' on ' || q.queue_tick_seq || ' to public';

    -- event seq
    execute 'grant ' || seq_perms
        || ' on ' || q.queue_event_seq || ' to public';
    
    -- parent table for events
    execute 'grant select on ' || q.queue_data_pfx || ' to public';

    -- real event tables
    for i in 0 .. q.queue_ntables - 1 loop
        execute 'grant ' || tbl_perms
            || ' on ' || q.queue_data_pfx || '_' || i
            || ' to public';
    end loop;

    return 1;
end;
$$ language plpgsql security definer;

