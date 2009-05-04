
create or replace function londiste.global_remove_table(
    in i_queue_name text, in i_table_name text,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.global_remove_table(2)
--
--      Removes tables registration in set.
--
--      Means that nodes cannot attach to this table anymore.
--
-- Called by:
--      - On root by londiste.local_remove_table()
--      - Elsewhere by consumer receiving table remove event
--
-- Returns:
--      200 - OK
--      400 - not found
-- ----------------------------------------------------------------------
declare
    fq_table_name text;
begin
    fq_table_name := londiste.make_fqname(i_table_name);
    if not pgq_node.is_root_node(i_queue_name) then
        perform londiste.local_remove_table(i_queue_name, fq_table_name);
    end if;
    delete from londiste.table_info
        where queue_name = i_queue_name
          and table_name = fq_table_name;
    if not found then
        select 400, 'Table not found: ' || fq_table_name
            into ret_code, ret_note;
        return;
    end if;
    select 200, 'Table removed: ' || i_table_name
        into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;

