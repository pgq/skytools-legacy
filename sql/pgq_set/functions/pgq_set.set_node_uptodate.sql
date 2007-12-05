
create or replace function pgq_set.set_node_uptodate(
    i_set_name text,
    i_uptodate boolean)
returns int4 as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.set_node_uptodate(2)
--
--      Set node uptodate flag.
--
-- Parameters:
--      i_set_name - set name
--      i_uptodate - new flag state
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
begin
    update pgq_set.set_info
       set up_to_date = i_uptodate
     where set_name = i_set_name;
    if not found then
        raise exception 'no such set: %', i_set_name;
    end if;
    return 1;
end;
$$ language plpgsql security definer;


