
create or replace function pgq_node.unregister_location(
    in i_queue_name text,
    in i_node_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.unregister_location(2)
--
--      Drop unreferenced node.
--
-- Parameters:
--      i_queue_name - queue name
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
    select * into node from pgq_node.node_info
      where queue_name = i_queue_name;
    if found then
        if node.node_name = i_node_name then
            select 403, 'Cannot drop nodes own location' into ret_code, ret_note;
            return;
        end if;
        if node.provider_node = i_node_name then
            select 403, 'Cannot drop location of nodes parent' into ret_code, ret_note;
            return;
        end if;
    end if;

    delete from pgq_node.node_location
     where queue_name = i_queue_name
       and node_name = i_node_name;
    if found then
        select 200, 'Ok' into ret_code, ret_note;
    else
        select 301, 'Location not found: ' || i_queue_name || '/' || i_node_name
          into ret_code, ret_note;
    end if;
    return;
end;
$$ language plpgsql security definer;

