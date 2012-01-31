
create or replace function pgq_node.set_consumer_uptodate(
    in i_queue_name text,
    in i_consumer_name text,
    in i_uptodate boolean,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_consumer_uptodate(3)
--
--      Set consumer uptodate flag.....
--
-- Parameters:
--      i_queue_name - queue name
--      i_consumer_name - consumer name
--      i_uptodate - new flag state
--
-- Returns:
--      200 - ok
--      404 - consumer not known
-- ----------------------------------------------------------------------
begin
    update pgq_node.local_state
       set uptodate = i_uptodate
     where queue_name = i_queue_name
       and consumer_name = i_consumer_name;
    if found then
        select 200, 'Consumer uptodate = ' || i_uptodate::int4::text
               into ret_code, ret_note;
    else
        select 404, 'Consumer not known: '
               || i_queue_name || '/' || i_consumer_name
          into ret_code, ret_note;
    end if;
    return;
end;
$$ language plpgsql security definer;


