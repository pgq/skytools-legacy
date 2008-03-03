create or replace function pgq_set.is_root(i_set_name text)
returns bool as $$
-- ----------------------------------------------------------------------
-- Function: pgq_set.is_root(1)
--
--      Checs if node is root.
--
-- Parameters:
--      i_set_name  - set name
-- ----------------------------------------------------------------------
declare
    res bool;
begin
    select n.node_type = 'root' into res
      from pgq_set.set_info n
      where n.set_name = i_set_name;
    if not found then
        raise exception 'set does not exist: %', i_set_name;
    end if;
    return res;
end;
$$ language plpgsql;

