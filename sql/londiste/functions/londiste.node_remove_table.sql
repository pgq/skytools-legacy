
create or replace function londiste.node_remove_table(
    in i_set_name text, in i_table_name text,
    out ret_code int4, out ret_desc text)
as $$
begin
    delete from londiste.node_table
        where set_name = i_set_name
          and table_name = i_table_name;
    if not found then
        select 400, 'Not found: '||i_table_name into ret_code, ret_desc;
        return;
    end if;

    -- perform londiste.provider_notify_change(i_queue_name);
    -- triggers
    select 200, 'OK' into ret_code, ret_desc;
    return;
end;
$$ language plpgsql strict;

