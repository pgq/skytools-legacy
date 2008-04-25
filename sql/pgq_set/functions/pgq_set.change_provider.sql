
create or replace function pgq_set.change_provider(
    in i_set_name text,
    in i_new_provider text,
    out ret_code int4,
    out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.change_provider(2)
--
--      Change provider for this node.
--
-- Parameters:
--      i_set_name  - set name
--      i_new_provider - node name for new provider
-- ----------------------------------------------------------------------
begin
    update pgq_set.set_info
       set provider_node = i_new_provider,
           uptodate = false
     where set_name = i_set_name;
    if not found then
        select 404, 'Unknown set: ' || i_set_name into ret_code, ret_note;
        return;
    end if;
    select 200, 'Node provider set to : ' || i_new_provider into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

