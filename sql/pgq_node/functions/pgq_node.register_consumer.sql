
create or replace function pgq_node.register_consumer(
    in i_queue_name text,
    in i_consumer_name text,
    in i_provider_node text,
    in i_custom_tick_id int8,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.register_consumer(4)
--
--      Subscribe plain cascaded consumer to a target node.
--      That means it's planning to read from remote node
--      and write to local node.
--
-- Parameters:
--      i_queue_name - set name
--      i_consumer_name - cascaded consumer name
--      i_provider_node - node name
--      i_custom_tick_id - tick id
--
-- Returns:
--      ret_code - error code
--      200 - ok
--      201 - already registered
--      401 - no such queue
--      ret_note - description
-- ----------------------------------------------------------------------
declare
    n record;
    node_wm_name text;
    node_pos bigint;
begin
    select node_type into n
      from pgq_node.node_info where queue_name = i_queue_name
       for update;
    if not found then
        select 404, 'Unknown queue: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;
    perform 1 from pgq_node.local_state
      where queue_name = i_queue_name
        and consumer_name = i_consumer_name;
    if found then
        update pgq_node.local_state
           set provider_node = i_provider_node,
               last_tick_id = i_custom_tick_id
         where queue_name = i_queue_name
           and consumer_name = i_consumer_name;
        select 201, 'Consumer already registered: ' || i_queue_name
               || '/' || i_consumer_name  into ret_code, ret_note;
        return;
    end if;

    insert into pgq_node.local_state (queue_name, consumer_name, provider_node, last_tick_id)
           values (i_queue_name, i_consumer_name, i_provider_node, i_custom_tick_id);

    select 200, 'Consumer '||i_consumer_name||' registered on queue '||i_queue_name
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

