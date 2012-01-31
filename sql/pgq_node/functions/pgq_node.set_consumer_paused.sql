
create or replace function pgq_node.set_consumer_paused(
    in i_queue_name text,
    in i_consumer_name text,
    in i_paused boolean,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.set_consumer_paused(3)
--
--      Set consumer paused flag.
--
-- Parameters:
--      i_queue_name - cascaded queue name
--      i_consumer_name - cascaded consumer name
--      i_paused   - new flag state
-- Returns:
--      200 - ok
--      201 - already paused
--      404 - consumer not found
-- ----------------------------------------------------------------------
declare
    old_flag    boolean;
    word        text;
begin
    if i_paused then
        word := 'paused';
    else
        word := 'resumed';
    end if;

    select paused into old_flag
        from pgq_node.local_state
        where queue_name = i_queue_name
          and consumer_name = i_consumer_name
        for update;
    if not found then
        select 404, 'Unknown consumer: ' || i_consumer_name
            into ret_code, ret_note;
    elsif old_flag = i_paused then
        select 201, 'Consumer ' || i_consumer_name || ' already ' || word
            into ret_code, ret_note;
    else
        update pgq_node.local_state
            set paused = i_paused,
                uptodate = false
            where queue_name = i_queue_name
            and consumer_name = i_consumer_name;

        select 200, 'Consumer '||i_consumer_name||' tagged as '||word into ret_code, ret_note;
    end if;
    return;

end;
$$ language plpgsql security definer;


