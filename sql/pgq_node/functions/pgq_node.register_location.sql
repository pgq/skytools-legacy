
create or replace function pgq_node.register_location(
    in i_queue_name text,
    in i_node_name text,
    in i_node_location text,
    in i_dead boolean,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.register_location(4)
--
--      Add new node location.
--
-- Parameters:
--      i_queue_name - queue name
--      i_node_name - node name
--      i_node_location - node connect string
--      i_dead - dead flag for node
--
-- Returns:
--      ret_code - error code
--      ret_note - error description
--
-- Return Codes:
--      200 - Ok
-- ----------------------------------------------------------------------
declare
    node record;
begin
    select node_type = 'root' as is_root into node
      from pgq_node.node_info where queue_name = i_queue_name
       for update;
    -- may return 0 rows

    perform 1 from pgq_node.node_location
     where queue_name = i_queue_name
       and node_name = i_node_name;
    if found then
        update pgq_node.node_location
           set node_location = coalesce(i_node_location, node_location),
               dead = i_dead
         where queue_name = i_queue_name
           and node_name = i_node_name;
    elsif i_node_location is not null then
        insert into pgq_node.node_location (queue_name, node_name, node_location, dead)
        values (i_queue_name, i_node_name, i_node_location, i_dead);
    end if;

    if node.is_root then
        perform pgq.insert_event(i_queue_name, 'pgq.location-info',
                                 i_node_name, i_queue_name, i_node_location, i_dead::text, null)
           from pgq_node.node_info n
         where n.queue_name = i_queue_name;
    end if;

    select 200, 'Location registered' into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

