
create or replace function londiste.set_remove_table(
    in i_set_name text, in i_table_name text,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.set_remove_table(2)
--
--      Removes tables registration in set.
--
--      Means that nodes cannot attach to this table anymore.
--
-- Called by:
--      - On root by londiste.node_remove_table()
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
    if not pgq_set.is_root(i_set_name) then
        perform londiste.node_remove_table(i_set_name, fq_table_name);
    end if;
    delete from londiste.set_table
        where set_name = i_set_name
          and table_name = fq_table_name;
    if not found then
        select 400, 'Not found: '||fq_table_name into ret_code, ret_note;
        return;
    end if;
    select 200, 'OK' into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;

