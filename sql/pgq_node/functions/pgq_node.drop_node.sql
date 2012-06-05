
create or replace function pgq_node.drop_node(
    in i_queue_name text,
    in i_node_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.drop_node(2)
--
--      Drop node. This needs to be run on all the members of a set
--      to properly get rid of the node.
--
-- Parameters:
--      i_queue_name - queue name
--      i_node_name - node_name
--
-- Returns:
--      ret_code - error code
--      ret_note - error description
--
-- Return Codes:
--      200 - Ok
--      304 - No such queue
--      406 - That is a provider
-- Calls:
--      None
-- Tables directly manipulated:
--      None
------------------------------------------------------------------------
declare
    _is_local   boolean;
    _is_prov    boolean;
begin
    select (n.node_name = i_node_name),
           (select s.provider_node = i_node_name
              from pgq_node.local_state s
              where s.queue_name = i_queue_name
                and s.consumer_name = n.worker_name)
        into _is_local, _is_prov
        from pgq_node.node_info n
        where n.queue_name = i_queue_name;

    if not found then
        -- proceed with cleaning anyway, as there schenarios
        -- where some data is left around
        _is_prov := false;
        _is_local := true;
    end if;

    -- drop local state
    if _is_local then
        delete from pgq_node.subscriber_info
         where queue_name = i_queue_name;

        delete from pgq_node.local_state
         where queue_name = i_queue_name;

        delete from pgq_node.node_info
         where queue_name = i_queue_name
            and node_name = i_node_name;

        perform pgq.drop_queue(queue_name, true)
           from pgq.queue where queue_name = i_queue_name;

        delete from pgq_node.node_location
         where queue_name = i_queue_name
           and node_name <> i_node_name;
    elsif _is_prov then
        select 405, 'Cannot drop provider node: ' || i_node_name into ret_code, ret_note;
        return;
    else
        perform pgq_node.unregister_subscriber(i_queue_name, i_node_name);
    end if;

    -- let the unregister_location send event if needed
    select f.ret_code, f.ret_note
        from pgq_node.unregister_location(i_queue_name, i_node_name) f
        into ret_code, ret_note;

    select 200, 'Node dropped: ' || i_node_name
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

