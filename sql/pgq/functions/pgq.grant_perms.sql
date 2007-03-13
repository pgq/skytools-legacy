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
    q     record;
    i     integer;
begin
    select * from pgq.queue into q
        where queue_name = x_queue_name;
    if not found then
        raise exception 'Queue not found';
    end if;
    execute 'grant select, update on '
        || q.queue_event_seq || ',' || q.queue_tick_seq
        || ' to public';
    execute 'grant select on '
        || q.queue_data_pfx
        || ' to public';
    for i in 0 .. q.queue_ntables - 1 loop
        execute 'grant select, insert on '
            || q.queue_data_pfx || '_' || i
            || ' to public';
    end loop;
    return 1;
end;
$$ language plpgsql security definer;

