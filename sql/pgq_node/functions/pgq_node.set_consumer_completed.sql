
create or replace function pgq_node.set_consumer_completed(
    in i_queue_name text,
    in i_consumer_name text,
    in i_tick_id int8,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_consumer_completed(3)
--
--      Set last completed tick id for the cascaded consumer
--      that it has committed to local node.
--
-- Parameters:
--      i_queue_name - cascaded queue name
--      i_consumer_name - cascaded consumer name
--      i_tick_id   - tick id
-- Returns:
--      200 - ok
--      404 - consumer not known
-- ----------------------------------------------------------------------
begin
    update pgq_node.local_state
       set last_tick_id = i_tick_id,
           cur_error = NULL
     where queue_name = i_queue_name
       and consumer_name = i_consumer_name;
    if found then
        select 100, 'Consumer ' || i_consumer_name || ' compleded tick = ' || i_tick_id::text
            into ret_code, ret_note;
    else
        select 404, 'Consumer not known: '
               || i_queue_name || '/' || i_consumer_name
          into ret_code, ret_note;
    end if;
    return;
end;
$$ language plpgsql security definer;


