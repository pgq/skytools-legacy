
create or replace function pgq_set.unsubscribe_node(
    in i_set_name text,
    in i_remote_node_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.unsubscribe_node(2)
--
--      Unsubscribe remote node from local node.
--
-- Parameters:
--      i_set_name - set name
--      i_remote_node_name - node name
--
-- Returns:
--      ret_code - error code
--      ret_note - description
-- ----------------------------------------------------------------------
declare
    s record;
    n record;
begin
    -- fetch node info
    select queue_name, node_type into n from pgq_set.set_info
     where set_name = i_set_name;
    if not found then
        select 404, 'No such set: '||i_set_name into ret_code, ret_note;
        return;
    end if;

    -- fetch subscription info
    select node_name into s from pgq_set.subscriber_info
     where set_name = i_set_name and node_name = i_remote_node_name
       for update;
    if not found then
        select 404, 'No such subscriber: '||i_remote_node_name into ret_code, ret_note;
        return;
    end if;

    -- unregister from queue
    perform pgq.unregister_consumer(n.queue_name, s.node_name);

    -- drop subscription
    delete from pgq_set.subscriber_info
     where set_name = i_set_name
       and node_name = i_remote_node_name;

    -- done
    select 200, 'Ok' into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

