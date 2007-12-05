
create or replace function pgq_set.set_global_watermark(
    i_set_name text,
    i_watermark bigint)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.set_global_watermark(2)
--
--      Move global watermark on branch/leaf.
--
-- Parameters:
--      i_set_name     - set name
--      i_watermark    - global tick_id that is processed everywhere
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    this        record;
    wm_consumer text;
begin
    select node_type, queue_name into this
        from pgq_set.set_info
        where set_name = i_set_name
        for update;
    if not found then
        raise exception 'set % not found', i_set_name;
    end if;

    update pgq_set.set_info
       set global_watermark = i_watermark
     where set_name = i_set_name;
    if not found then
        raise exception 'node % not subscribed to set %', i_node_name, i_set_name;
    end if;

    -- move watermark on pgq
    if this.queue_name is not null then
        wm_consumer := i_set_name || '.watermark';
        perform pgq.register_consumer_at(this.queue_name, wm_consumer, i_watermark);
    end if;

    if this.node_type in ('root', 'combined-root') then
        perform pgq.insert_event(this.queue_name, 'global-watermark', i_watermark,
                                 i_set_name, null, null, null);
    end if;
    return i_watermark;
end;
$$ language plpgsql security definer;


