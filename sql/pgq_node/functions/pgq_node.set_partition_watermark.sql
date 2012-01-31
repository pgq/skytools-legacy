
create or replace function pgq_node.set_partition_watermark(
    in i_combined_queue_name text,
    in i_part_queue_name text,
    in i_watermark bigint,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_partition_watermark(3)
--
--      Move merge-leaf position on combined-branch.
--
-- Parameters:
--      i_combined_queue_name - local combined queue name
--      i_part_queue_name     - local part queue name (merge-leaf)
--      i_watermark         - partition tick_id that came inside combined-root batch
--
-- Returns:
--      200 - success
--      201 - no partition queue
--      401 - worker registration not found
-- ----------------------------------------------------------------------
declare
    n record;
begin
    -- check if combined-branch exists
    select c.node_type, p.worker_name into n
        from pgq_node.node_info c, pgq_node.node_info p
        where p.queue_name = i_part_queue_name
          and c.queue_name = i_combined_queue_name
          and p.combined_queue = c.queue_name
          and p.node_type = 'leaf'
          and c.node_type = 'branch';
    if not found then
        select 201, 'Part-queue does not exist' into ret_code, ret_note;
        return;
    end if;

    update pgq_node.local_state
       set last_tick_id = i_watermark
     where queue_name = i_part_queue_name
       and consumer_name = n.worker_name;
    if not found then
        select 401, 'Worker registration not found' into ret_code, ret_note;
        return;
    end if;

    select 200, 'Ok' into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;


