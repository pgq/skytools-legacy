
create or replace function pgq_set.set_subscriber_watermark(
    i_set_name text,
    i_node_name text,
    i_watermark bigint)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.set_subscriber_watermark(3)
--
--      Notify provider about subscribers lowest watermark.
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
    update pgq_set.subscriber_info
       set local_watermark = i_watermark
     where set_name = i_set_name
       and node_name = i_node_name;
    if not found then
        raise exception 'node % not subscribed to set %', i_node_name, i_set_name;
    end if;

    return i_watermark;
end;
$$ language plpgsql security definer;


