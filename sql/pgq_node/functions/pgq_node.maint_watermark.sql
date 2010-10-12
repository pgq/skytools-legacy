
create or replace function pgq_node.maint_watermark(i_queue_name text)
returns int4 as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.maint_watermark(1)
--
--      Move global watermark on root node.
--
-- Returns:
--      0 - tells pgqd to call just once
-- ----------------------------------------------------------------------
declare
    _lag interval;
begin
    perform 1 from pgq_node.node_info
      where queue_name = i_queue_name
        and node_type = 'root'
      for update;
    if not found then
        return 0;
    end if;

    select lag into _lag from pgq.get_consumer_info(i_queue_name, '.global_watermark');
    if _lag >= '5 minutes'::interval then
        perform pgq_node.set_global_watermark(i_queue_name, NULL);
    end if;

    return 0;
end;
$$ language plpgsql;

