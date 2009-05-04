
create or replace function pgq_node.unregister_subscriber(
    in i_queue_name text,
    in i_remote_node_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.unregister_subscriber(2)
--
--      Unsubscribe remote node from local node.
--
-- Parameters:
--      i_queue_name - set name
--      i_remote_node_name - node name
--
-- Returns:
--      ret_code - error code
--      ret_note - description
-- ----------------------------------------------------------------------
declare
    n_wm_name text;
    worker_name text;
begin
    n_wm_name := '.' || i_remote_node_name || '.watermark';
    select s.worker_name into worker_name from pgq_node.subscriber_info s
        where queue_name = i_queue_name and subscriber_node = i_remote_node_name;
    if not found then
        select 304, 'Subscriber not found' into ret_code, ret_note;
        return;
    end if;

    perform pgq.unregister_consumer(i_queue_name, n_wm_name);
    perform pgq.unregister_consumer(i_queue_name, worker_name);

    delete from pgq_node.subscriber_info
        where queue_name = i_queue_name
            and subscriber_node = i_remote_node_name;

    select 200, 'Subscriber unregistered: '||i_remote_node_name
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

