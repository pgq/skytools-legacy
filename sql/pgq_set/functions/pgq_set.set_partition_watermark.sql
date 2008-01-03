
create or replace function pgq_set.set_partition_watermark(
    i_combined_set_name text,
    i_part_set_name text,
    i_watermark bigint)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.set_partition_watermark(3)
--
--      Move merge-leaf position on combined-branch.
--
-- Parameters:
--      i_combined_set_name - local combined set name
--      i_part_set_name     - local part set name (merge-leaf)
--      i_watermark         - partition tick_id that came inside combined-root batch
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    cnode       record;
    pnode       record;
    part_worker text;
begin
    -- check if combined-branch exists
    select p.worker_name into part_worker
        from pgq_set.set_info c, pgq_set.set_info p
        where p.set_name = i_part_set_name
          and c.set_name = i_combined_set_name
          and p.combined_set = c.set_name
          and p.node_type = 'merge-leaf'
          and c.node_type = 'combined-branch';
    if not found then
        raise exception 'combined-branch/merge-leaf pair not found (%/%)', i_combined_set_name, i_part_set_name;
    end if;


    update pgq_set.completed_tick
       set tick_id = i_watermark
     where set_name = i_part_set_name
       and worker_name = part_worker;
    if not found then
        raise exception 'node % not subscribed to set %', i_node_name, i_set_name;
    end if;

    return i_watermark;
end;
$$ language plpgsql security definer;


