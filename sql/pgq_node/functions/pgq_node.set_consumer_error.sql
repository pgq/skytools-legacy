
create or replace function pgq_node.set_consumer_error(
    in i_queue_name text,
    in i_consumer_name text,
    in i_error_msg text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_consumer_error(3)
--
--      If batch processing fails, consumer can store it's last error in db.
-- Returns:
--      100 - ok
--      101 - consumer not known
-- ----------------------------------------------------------------------
begin
    update pgq_node.local_state
       set cur_error = i_error_msg
     where queue_name = i_queue_name
       and consumer_name = i_consumer_name;
    if found then
        select 100, 'Consumer ' || i_consumer_name || ' error = ' || i_error_msg
            into ret_code, ret_note;
    else
        select 101, 'Consumer not known, ignoring: '
               || i_queue_name || '/' || i_consumer_name
          into ret_code, ret_note;
    end if;
    return;
end;
$$ language plpgsql security definer;


