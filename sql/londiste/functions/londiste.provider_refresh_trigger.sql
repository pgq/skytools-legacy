
create or replace function londiste.provider_refresh_trigger(
    i_queue_name    text,
    i_table_name    text,
    i_col_types     text
) returns integer strict as $$
declare
    t_name   text;
    tbl_oid  oid;
begin
    select trigger_name into t_name
        from londiste.provider_table
        where queue_name = i_queue_name
          and table_name = i_table_name;
    if not found then
        raise exception 'table not found';
    end if;

    tbl_oid := londiste.find_table_oid(i_table_name);
    perform 1 from pg_trigger
        where tgrelid = tbl_oid
          and tgname = t_name;
    if found then
        execute 'drop trigger ' || quote_ident(t_name)
            || ' on ' || londiste.quote_fqname(i_table_name);
    end if;

    perform londiste.provider_create_trigger(i_queue_name, i_table_name, i_col_types);

    return 1;
end;
$$ language plpgsql security definer;

create or replace function londiste.provider_refresh_trigger(
    i_queue_name    text,
    i_table_name    text
) returns integer strict as $$
begin
    return londiste.provider_refresh_trigger(i_queue_name, i_table_name,
                            londiste.find_column_types(i_table_name));
end;
$$ language plpgsql security definer;



