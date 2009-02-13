
create or replace function pgq_node.register_subscriber(
    in i_queue_name text,
    in i_remote_node_name text,
    in i_remote_worker_name text,
    in i_custom_tick_id int8,
    out ret_code int4,
    out ret_note text,
    out global_watermark bigint)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.register_subscriber(4)
--
--      Subscribe remote node to local node at custom position.
--      Should be used when changing provider for existing node.
--
-- Parameters:
--      i_node_name - set name
--      i_remote_node_name - node name
--      i_remote_worker_name - consumer name
--      i_custom_tick_id - tick id [optional]
--
-- Returns:
--      ret_code - error code
--      ret_note - description
--      global_watermark - minimal watermark
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
    select last_tick into global_watermark
      from pgq.get_consumer_info(i_queue_name, '.global_watermark');

    if n.node_type not in ('root', 'branch') then
        select 401, 'Cannot subscribe to ' || n.node_type || ' node'
          into ret_code, ret_note;
        return;
    end if;

    node_wm_name := '.' || i_remote_node_name || '.watermark';
    node_pos := coalesce(i_custom_tick_id, global_watermark);

    perform pgq.register_consumer_at(i_queue_name, node_wm_name, global_watermark);

    perform pgq.register_consumer_at(i_queue_name, i_remote_worker_name, node_pos);

    insert into pgq_node.subscriber_info (queue_name, subscriber_node, worker_name, watermark_name)
        values (i_queue_name, i_remote_node_name, i_remote_worker_name, node_wm_name);

    select 200, 'Subscriber registered: '||i_remote_node_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

