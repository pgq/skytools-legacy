
create or replace function pgq_set.create_node(
    in i_set_name text,
    in i_node_type text,
    in i_node_name text,
    in i_provider_name text,
    in i_global_watermark bigint,
    in i_combined_set text,
    out ret_code int4,
    out ret_desc text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.create_node(6)
--
--      Initialize node.
--
-- Parameters:
--      i_set_name - set name
--      i_node_type - node type
--      i_node_name - node name
--      i_provider_name - provider node name for non-root nodes
--      i_global_watermark - global lowest tick_id
--      i_combined_set - merge-leaf: target set
--
-- Returns:
--      desc
--
-- Node Types:
--      root - master node
--      branch - subscriber node that can be provider to others
--      leaf - subscriber node that cannot be provider to others
--      combined-root - root node for combined set
--      combined-branch - failover node for combined set
--      merge-leaf - leaf node on partition set that will be merged into combined-root
-- ----------------------------------------------------------------------
declare
    _queue_name text;
    _wm_consumer text;
    _global_wm bigint;
begin
    if i_node_type in ('root', 'combined-root') then
        if coalesce(i_provider_name, i_global_watermark::text,
                    i_combined_set) is not null then
            raise exception 'unexpected args for %', i_node_type;
        end if;

        _queue_name := i_set_name;
        _wm_consumer := i_set_name || '.watermark';
        perform pgq.create_queue(_queue_name);
        perform pgq.register_consumer(_queue_name, _wm_consumer);
        _global_wm := (select last_tick from pgq.get_consumer_info(_queue_name, _wm_consumer));
    elsif i_node_type in ('branch', 'combined-branch') then
        if i_provider_name is null then
            raise exception 'provider not set for %', i_node_type;
        end if;
        if i_global_watermark is null then
            raise exception 'global_watermark not set for %', i_node_type;
        end if;
        if i_node_type = 'combined-branch' and i_combined_set is null then
            raise exception 'combined-set not given for %', i_node_type;
        end if;
        _queue_name := i_set_name;
        _wm_consumer := i_set_name || '.watermark';
        perform pgq.create_queue(_queue_name);
        update pgq.queue set queue_external_ticker = true where queue_name = _queue_name;
        if i_global_watermark > 1 then
            perform pgq.ticker(_queue_name, i_global_watermark);
        end if;
        perform pgq.register_consumer_at(_queue_name, _wm_consumer, i_global_watermark);
        _global_wm := i_global_watermark;
    elsif i_node_type in ('leaf', 'merge-leaf') then
        _queue_name := null;
        _global_wm := i_global_watermark;
    end if;

    insert into pgq_set.set_info
      (set_name, node_type, node_name, queue_name,
       provider_node, combined_set, global_watermark)
    values (i_set_name, i_node_type, i_node_name, _queue_name,
       i_provider_name, i_combined_set, _global_wm);

    if i_node_type not in ('root', 'combined-root') then
        insert into pgq_set.completed_tick (set_name, tick_id)
            values (i_set_name, _global_wm);
    end if;

    select 200, 'Ok' into ret_code, ret_desc;
    return;
end;
$$ language plpgsql security definer;

