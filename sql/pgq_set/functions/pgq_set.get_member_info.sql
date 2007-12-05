
create or replace function pgq_set.get_member_info(
    in i_set_name text,

    out node_name text,
    out node_location text,
    out dead boolean
) returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.get_member_info(1)
--
--      Get member list for the set.
--
-- Parameters:
--      i_set_name  - set name
--
-- Returns:
--      node_name       - node name
--      node_location   - libpq connect string for the node
--      dead            - whether the node should be considered dead
-- ----------------------------------------------------------------------
begin
    for node_name, node_location, dead in
        select m.node_name, m.node_location, m.dead
          from pgq_set.member_info m
         where m.set_name = i_set_name
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer;

