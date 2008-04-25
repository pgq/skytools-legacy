
create or replace function pgq_set.drop_member(
    in i_set_name text,
    in i_node_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.drop_member(1)
--
--      Drop unreferenced member.
--
-- Parameters:
--      i_set_name - set name
--      i_node_name - node to drop
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
    _queue_name  text;
    _wm_consumer text;
    _global_wm   bigint;
    sub          record;
    node         record;
begin
    select * into node from pgq_set.set_info
      where set_name = i_set_name;
    if not found then
        select 404, 'No such set: ' || i_set_name into ret_code, ret_note;
        return;
    end if;
    if node.node_name = i_node_name then
        select 403, 'Cannot use drop_member on node itself' into ret_code, ret_note;
        return;
    end if;
    if node.provider_node = i_node_name then
        select 403, 'Cannot use drop_member on node child' into ret_code, ret_note;
        return;
    end if;

    perform 1 from pgq_set.subscriber_info
      where set_name = i_set_name
        and node_name = i_node_name;
    if found then
        select f.ret_code, f.ret_note into ret_code, ret_note
          from pgq_set.unsubscribe_node(i_set_name, i_node_name) f;
    end if;
    perform * from pgq_set.remove_member(i_set_name, i_node_name);
    select 200, 'Ok' into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

