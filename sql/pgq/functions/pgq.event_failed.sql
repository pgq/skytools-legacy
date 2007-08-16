create or replace function pgq.event_failed(
    x_batch_id bigint,
    x_event_id bigint,
    x_reason text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgq.event_failed(3)
--
--      Copies the event to failed queue so it can be looked at later.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--      x_event_id      - Event id
--      x_reason        - Text to associate with event.
--
-- Returns:
--     0 if event was already in queue, 1 otherwise.
-- ----------------------------------------------------------------------
begin
    insert into pgq.failed_queue (ev_failed_reason, ev_failed_time,
        ev_id, ev_time, ev_txid, ev_owner, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    select x_reason, now(),
           ev_id, ev_time, NULL, sub_id, coalesce(ev_retry, 0),
           ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4
      from pgq.get_batch_events(x_batch_id),
           pgq.subscription
     where sub_batch = x_batch_id
       and ev_id = x_event_id;
    if not found then
        raise exception 'event not found';
    end if;
    return 1;

-- dont worry if the event is already in queue
exception
    when unique_violation then
        return 0;
end;
$$ language plpgsql security definer;

