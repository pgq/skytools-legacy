
create or replace function pgq.get_batch_cursor(
    in i_batch_id       bigint,
    in i_cursor_name    text,
    in i_quick_limit    int4,

    out ev_id       bigint,
    out ev_time     timestamptz,
    out ev_txid     bigint,
    out ev_retry    int4,
    out ev_type     text,
    out ev_data     text,
    out ev_extra1   text,
    out ev_extra2   text,
    out ev_extra3   text,
    out ev_extra4   text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq.get_batch_cursor(3)
--
--      Get events in batch using a cursor.
--
-- Parameters:
--      i_batch_id      - ID of active batch.
--      i_cursor_name   - Name for new cursor
--      i_quick_limit   - Number of events to return immediately
--
-- Returns:
--      List of events.
-- ----------------------------------------------------------------------
declare
    _cname  text;
    _rcnt   int4;
begin
    _cname := quote_ident(i_cursor_name);

    -- create cursor
    execute 'declare ' || _cname
        || ' no scroll cursor for '
        || pgq.batch_event_sql(i_batch_id);

    -- if no events wanted, don't bother with execute
    if i_quick_limit <= 0 then
        return;
    end if;

    -- return first block of events
    _rcnt := 0;
    for ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4
        in execute 'fetch ' || i_quick_limit || ' from ' || _cname
    loop
        _rcnt := _rcnt + 1;
        return next;
    end loop;

    -- close cursor if all events have been returned
    if _rcnt < i_quick_limit then
        execute 'close ' || _cname;
    end if;

    return;
end;
$$ language plpgsql strict; -- no perms needed

