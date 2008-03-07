
create or replace function londiste.node_get_table_list(
    in i_set_name text,
    out table_name text,
    out merge_state text,
    out custom_snapshot text,
    out skip_truncate bool)
returns setof record as $$ 
-- ----------------------------------------------------------------------
-- Function: londiste.node_get_table_list(1)
--
--      Return info about registered tables.
--
-- Parameters:
--      i_set_name - set name
-- ----------------------------------------------------------------------
begin 
    for table_name, merge_state, custom_snapshot, skip_truncate in 
        select t.table_name, t.merge_state, t.custom_snapshot, t.skip_truncate
            from londiste.node_table t
            where t.set_name= i_set_name
            order by t.nr
    loop
        return next;
    end loop; 
    return;
end; 
$$ language plpgsql strict stable;

