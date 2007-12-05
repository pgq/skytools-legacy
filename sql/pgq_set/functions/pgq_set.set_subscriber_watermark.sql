
create or replace function pgq_set.set_subscriber_watermark(
    i_set_name text,
    i_node_name text,
    i_watermark bigint)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.set_subscriber_watermark(3)
--
--      Notify provider about subscribers lowest watermark.  On root
--      node, changes global_watermark and sends event about that
--      to the queue.
--
-- Parameters:
--      i_set_name - set name
--      i_node_name - subscriber node name
--      i_watermark - tick_id
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    m       record;
    cur_wm  bigint;
begin
    select node_type, global_watermark, local_queue
      into m
      from pgq_set.local_node
     where set_name = i_set_name
       for update;
    if not found then
        raise exception 'no such set: %', i_set_name;
    end if;

    update pgq_set.subscriber_info
       set local_watermark = i_watermark
     where set_name = i_set_name
       and node_name = i_node_name;
    if not found then
        raise exception 'node % not subscribed to set %', i_node_name, i_set_name;
    end if;

    if m.node_type in ('root', 'combined-root') then
        cur_wm := pgq_set.get_local_watermark(i_set_name);
        if cur_wm > m.global_watermark then
            update pgq_set.local_node set global_watermark = cur_wm
                where set_name = i_set_name;
            perform pgq.insert_event(m.local_queue, 'global-watermark', cur_wm);
        end if;
    end if;

    return i_watermark;
end;
$$ language plpgsql security definer;


