
create or replace function pgq_set.drop_node(
    in i_set_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.drop_node(1)
--
--      Drop node.
--
-- Parameters:
--      i_set_name - set name
--
-- Returns:
--      ret_code - error code
--      ret_note - error description
--
-- Return Codes:
--      200 - Ok
--      404 - No such set
-- ----------------------------------------------------------------------
declare
    _queue_name text;
    _wm_consumer text;
    _global_wm bigint;
    sub record;
begin
    perform 1 from pgq_set.set_info
      where set_name = i_set_name;
    if not found then
        select 404, 'No such set: ' || i_set_name into ret_code, ret_note;
        return;
    end if;

    perform pgq_set.unsubscribe_node(s.set_name, s.node_name)
       from pgq_set.subscriber_info s
      where set_name = i_set_name;

    delete from pgq_set.completed_tick
     where set_name = i_set_name;

    delete from pgq_set.set_info
     where set_name = i_set_name;

    delete from pgq_set.member_info
     where set_name = i_set_name;

    select 200, 'Ok' into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

