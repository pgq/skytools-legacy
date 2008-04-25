
create or replace function pgq_set.subscribe_node(
    in i_set_name text,
    in i_remote_node_name text,
    in i_custom_tick_id int8,
    out ret_code int4,
    out ret_note text,
    out global_watermark bigint)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.subscribe_node(3)
--
--      Subscribe remote node to local node at custom position.
--      Should be used when changing provider for existing node.
--
-- Parameters:
--      i_set_name - set name
--      i_remote_node_name - node name
--      i_custom_tick_id - tick id
--
-- Returns:
--      ret_code - error code
--      ret_note - description
--      global_watermark - minimal watermark
-- ----------------------------------------------------------------------
declare
    n record;
begin
    select s.node_type, s.global_watermark, s.queue_name into n
      from pgq_set.set_info s where s.set_name = i_set_name
       for update;
    if not found then
        select 404, 'Unknown set: ' || i_set_name into ret_code, ret_note;
        return;
    end if;
    global_watermark := n.global_watermark;

    if n.node_type in ('leaf', 'merge-leaf') then
        select 401, 'Cannot subscribe to ' || n.node_type || ' node'
          into ret_code, ret_note;
        return;
    end if;

    perform pgq.register_consumer_at(n.queue_name, i_remote_node_name, i_custom_tick_id);

    insert into pgq_set.subscriber_info (set_name, node_name, local_watermark)
    values (i_set_name, i_remote_node_name, n.global_watermark);

    select 200, 'Ok' into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

create or replace function pgq_set.subscribe_node(
    in i_set_name text,
    in i_remote_node_name text,
    out ret_code int4,
    out ret_note text,
    out global_watermark bigint)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.subscribe_node(2)
--
--      Subscribe remote node to local node at global_watermark position,
--      so it receivec all events.
--
-- Parameters:
--      i_set_name - set name
--      i_remote_node_name - node name
--
-- Returns:
--      ret_code - error code
--      ret_note - description
--      global_watermark - minimal watermark, also subscription pos
-- ----------------------------------------------------------------------
declare
    n record;
begin
    select s.global_watermark into n
      from pgq_set.set_info s where s.set_name = i_set_name
       for update;

    select f.ret_code, f.ret_note, f.global_watermark
      into ret_code, ret_note, global_watermark
      from pgq_set.subscribe_node(i_set_name, i_remote_node_name, n.global_watermark) f;

    return;
end;
$$ language plpgsql security definer;

