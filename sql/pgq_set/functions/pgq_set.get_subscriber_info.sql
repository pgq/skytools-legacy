
create or replace function pgq_set.get_subscriber_info(
    in i_set_name text,

    out node_name text,
    out local_watermark int8)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.get_subscriber_info(1)
--
--      Get subscriber list for the set.
--
-- Parameters:
--      i_set_name  - set name
--
-- Returns:
--      node_name       - node name
--      local_watermark - lowest tick_id on subscriber
-- ----------------------------------------------------------------------
begin
    for node_name, local_watermark in
        select s.node_name, s.local_watermark
          from pgq_set.subscriber_info s
         where s.set_name = i_set_name
         order by 1
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer;

