
create or replace function pgq_node.create_node(
    in i_queue_name text,
    in i_node_type text,
    in i_node_name text,
    in i_worker_name text,
    in i_provider_name text,
    in i_global_watermark bigint,
    in i_combined_queue text,
    out ret_code int4,
    out ret_note  text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.create_node(7)
--
--      Initialize node.
--
-- Parameters:
--      i_node_name - cascaded queue name
--      i_node_type - node type
--      i_node_name - node name
--      i_worker_name - worker consumer name
--      i_provider_name - provider node name for non-root nodes
--      i_global_watermark - global lowest tick_id
--      i_combined_queue - merge-leaf: target queue
--
-- Returns:
--      200 - Ok
--      401 - node already initialized
--      ???? - maybe we coud use more error codes ?
--
-- Node Types:
--      root - master node
--      branch - subscriber node that can be provider to others
--      leaf - subscriber node that cannot be provider to others
-- Calls:
--      None
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
declare
    _wm_consumer text;
    _global_wm bigint;
begin
    perform 1 from pgq_node.node_info where queue_name = i_queue_name;
    if found then
        select 401, 'Node already initialized' into ret_code, ret_note;
        return;
    end if;

    _wm_consumer := '.global_watermark';

    if i_node_type = 'root' then
        if coalesce(i_provider_name, i_global_watermark::text,
                    i_combined_queue) is not null then
            select 401, 'unexpected args for '||i_node_type into ret_code, ret_note;
            return;
        end if;

        perform pgq.create_queue(i_queue_name);
        perform pgq.register_consumer(i_queue_name, _wm_consumer);
        _global_wm := (select last_tick from pgq.get_consumer_info(i_queue_name, _wm_consumer));
    elsif i_node_type = 'branch' then
        if i_provider_name is null then
            select 401, 'provider not set for '||i_node_type into ret_code, ret_note;
            return;
        end if;
        if i_global_watermark is null then
            select 401, 'global watermark not set for '||i_node_type into ret_code, ret_note;
            return;
        end if;
        perform pgq.create_queue(i_queue_name);
        update pgq.queue
            set queue_external_ticker = true,
                queue_disable_insert = true
            where queue_name = i_queue_name;
        if i_global_watermark > 1 then
            perform pgq.ticker(i_queue_name, i_global_watermark, now(), 1);
        end if;
        perform pgq.register_consumer_at(i_queue_name, _wm_consumer, i_global_watermark);
        _global_wm := i_global_watermark;
    elsif i_node_type = 'leaf' then
        _global_wm := i_global_watermark;
        if i_combined_queue is not null then
            perform 1 from pgq.get_queue_info(i_combined_queue);
            if not found then
                select 401, 'non-existing queue on leaf side: '||i_combined_queue
                into ret_code, ret_note;
                return;
            end if;
        end if;
    else
        select 401, 'bad node type: '||i_node_type
          into ret_code, ret_note;
    end if;

    insert into pgq_node.node_info
      (queue_name, node_type, node_name,
       worker_name, combined_queue)
    values (i_queue_name, i_node_type, i_node_name,
       i_worker_name, i_combined_queue);

    if i_node_type <> 'root' then
        select f.ret_code, f.ret_note into ret_code, ret_note
          from pgq_node.register_consumer(i_queue_name, i_worker_name,
                    i_provider_name, _global_wm) f;
    else
        select f.ret_code, f.ret_note into ret_code, ret_note
          from pgq_node.register_consumer(i_queue_name, i_worker_name,
                    i_node_name, _global_wm) f;
    end if;
        if ret_code <> 200 then
            return;
        end if;

    select 200, 'Node "' || i_node_name || '" initialized for queue "'
           || i_queue_name || '" with type "' || i_node_type || '"'
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

