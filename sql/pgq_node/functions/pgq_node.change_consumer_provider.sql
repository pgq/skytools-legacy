
create or replace function pgq_node.change_consumer_provider(
    in i_queue_name text,
    in i_consumer_name text,
    in i_new_provider text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.change_consumer_provider(3)
--
--      Change provider for this consumer.
--
-- Parameters:
--      i_queue_name  - queue name
--      i_consumer_name  - consumer name
--      i_new_provider - node name for new provider
-- Returns:
--      ret_code - error code
--      200 - ok
--      404 - no such consumer or new node
--      ret_note - description
-- ----------------------------------------------------------------------
begin
    perform 1 from pgq_node.node_location
      where queue_name = i_queue_name
        and node_name = i_new_provider;
    if not found then
        select 404, 'New node not found: ' || i_new_provider
          into ret_code, ret_note;
        return;
    end if;

    update pgq_node.local_state
       set provider_node = i_new_provider,
           uptodate = false
     where queue_name = i_queue_name
       and consumer_name = i_consumer_name;
    if not found then
        select 404, 'Unknown consumer: ' || i_queue_name || '/' || i_consumer_name
          into ret_code, ret_note;
        return;
    end if;
    select 200, 'Consumer provider node set to : ' || i_new_provider
      into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

