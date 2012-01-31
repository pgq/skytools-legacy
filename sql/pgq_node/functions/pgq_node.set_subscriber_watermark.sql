
create or replace function pgq_node.set_subscriber_watermark(
    in i_queue_name text,
    in i_node_name text,
    in i_watermark bigint,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_subscriber_watermark(3)
--
--      Notify provider about subscribers lowest watermark.
--
--      Called on provider at interval by each worker  
--
-- Parameters:
--      i_queue_name - cascaded queue name
--      i_node_name - subscriber node name
--      i_watermark - tick_id
--
-- Returns:
--      ret_code    - error code
--      ret_note    - description
-- ----------------------------------------------------------------------
declare
    n       record;
    wm_name text;
begin
    wm_name := '.' || i_node_name || '.watermark';
    select * into n from pgq.get_consumer_info(i_queue_name, wm_name);
    if not found then
        select 404, 'node '||i_node_name||' not subscribed to queue ', i_queue_name
            into ret_code, ret_note;
        return;
    end if;

    -- todo: check if wm sane?
    if i_watermark < n.last_tick then
        select 405, 'watermark must not be moved backwards'
            into ret_code, ret_note;
        return;
    elsif i_watermark = n.last_tick then
        select 100, 'watermark already set'
            into ret_code, ret_note;
        return;
    end if;

    perform pgq.register_consumer_at(i_queue_name, wm_name, i_watermark);

    select 200, wm_name || ' set to ' || i_watermark::text
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;


