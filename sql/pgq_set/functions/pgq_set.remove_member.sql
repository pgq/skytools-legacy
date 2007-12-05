
create or replace function pgq_set.remove_member(
    in i_set_name text,
    in i_node_name text,
    out ret_code int4,
    out ret_note text)
returns record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.remove_member(2)
--
--      Add new set member.
--
-- Parameters:
--      i_set_name - set name
--      i_node_name - node name
--
-- Returns:
--      ret_code - error code
--      ret_note - error description
--
-- Return Codes:
--      200 - Ok
-- ----------------------------------------------------------------------
declare
    o  record;
begin
    delete from pgq_set.member_info
     where set_name = i_set_name
       and node_name = i_node_name;
    select 200, 'Ok' into ret_code, ret_note;
    return;
end;
$$ language plpgsql security definer;

