
create or replace function pgq_set.set_node_paused(
    in i_set_name text,
    in i_paused boolean,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.set_node_paused(2)
--
--      Set node paused flag.
--
-- Parameters:
--      i_set_name - set name
--      i_paused   - new flag state
-- ----------------------------------------------------------------------
declare
    cur_paused  boolean;
    nname       text;
    op          text;
begin
    op := case when i_paused then 'paused' else 'resumed' end;
    select paused, node_name into cur_paused, nname
      from pgq_set.set_info
     where set_name = i_set_name
       for update;
    if not found then
        select 400, 'Set does not exist: ' || i_set_name into ret_code, ret_note;
        return;
    end if;

    if cur_paused = i_paused then
        select 200, 'Node already '|| op || ': ' || nname into ret_code, ret_note;
        return;
    end if;

    update pgq_set.set_info
       set paused = i_paused,
           uptodate = false
     where set_name = i_set_name;
    select 200, 'Node ' || op || ': ' || nname into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;


