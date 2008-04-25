
create or replace function londiste.node_remove_table(
    in i_set_name text, in i_table_name text,
    out ret_code int4, out ret_note text)
as $$
declare
    fq_table_name text;
begin
    fq_table_name := londiste.make_fqname(i_table_name);

    for ret_code, ret_note in
        select f.ret_code, f.ret_note from londiste.node_disable_triggers(i_set_name, fq_table_name) f
    loop
        if ret_code > 299 then
            return;
        end if;
    end loop;
    delete from londiste.node_trigger
        where set_name = i_set_name
          and table_name = fq_table_name;
    delete from londiste.node_table
        where set_name = i_set_name
          and table_name = fq_table_name;
    if not found then
        select 400, 'Not found: ' || fq_table_name into ret_code, ret_note;
        return;
    end if;

    if pgq_set.is_root(i_set_name) then
        perform londiste.set_remove_table(i_set_name, fq_table_name);
        perform londiste.root_notify_change(i_set_name, 'remove-table', fq_table_name);
    end if;

    select 200, 'Table removed: ' || fq_table_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;

