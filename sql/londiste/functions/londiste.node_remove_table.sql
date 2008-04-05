
create or replace function londiste.node_remove_table(
    in i_set_name text, in i_table_name text,
    out ret_code int4, out ret_desc text)
as $$
declare
    fq_table_name text;
begin
    fq_table_name := londiste.make_fqname(i_table_name);
    delete from londiste.node_table
        where set_name = i_set_name
          and table_name = fq_table_name;
    if not found then
        select 400, 'Not found: ' || fq_table_name into ret_code, ret_desc;
        return;
    end if;

    -- perform londiste.provider_notify_change(i_queue_name);
    -- triggers
    select 200, 'Table removed: ' || fq_table_name into ret_code, ret_desc;
    return;
end;
$$ language plpgsql strict;

