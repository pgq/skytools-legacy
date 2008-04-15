
create or replace function londiste.set_get_table_list(
    in i_set_name       text,
    out table_name      text,
    out is_local        bool)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: londiste.set_get_table_list(1)
--
--      Show tables registered for set.
--
--      This means its available from root, events for it appear
--      in queue and nodes can attach to it.
--
-- Called by:
--      Admin tools.
-- ----------------------------------------------------------------------
begin
    for table_name, is_local in
        select t.table_name, n.table_name is not null
          from londiste.set_table t left join londiste.node_table n
               on (t.set_name = n.set_name and t.table_name = n.table_name)
         where t.set_name = i_set_name
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql strict security definer;

