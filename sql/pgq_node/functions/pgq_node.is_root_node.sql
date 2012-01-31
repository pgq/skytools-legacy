create or replace function pgq_node.is_root_node(i_queue_name text)
returns bool as $$
-- ----------------------------------------------------------------------
-- Function: pgq_node.is_root_node(1)
--
--      Checs if node is root.
--
-- Parameters:
--      i_queue_name  - queue name
-- Returns:
--      true - if this this the root node for queue 
-- ----------------------------------------------------------------------
declare
    res bool;
begin
    select n.node_type = 'root' into res
      from pgq_node.node_info n
      where n.queue_name = i_queue_name;
    if not found then
        raise exception 'queue does not exist: %', i_queue_name;
    end if;
    return res;
end;
$$ language plpgsql;

