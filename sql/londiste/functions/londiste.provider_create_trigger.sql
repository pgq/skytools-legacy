
create or replace function londiste.provider_create_trigger(
    i_queue_name    text,
    i_table_name    text,
    i_col_types     text
) returns integer strict as $$
declare
    tgname text;
begin
    select trigger_name into tgname
        from londiste.provider_table
        where queue_name = i_queue_name
          and table_name = i_table_name;
    if not found then
        raise exception 'table not found';
    end if;

    execute 'create trigger ' || quote_ident(tgname)
        || ' after insert or update or delete on '
        || londiste.quote_fqname(i_table_name)
        || ' for each row execute procedure pgq.logtriga('
        || quote_literal(i_queue_name) || ', '
        || quote_literal(i_col_types) || ', '
        || quote_literal(i_table_name) || ')';

    return 1;
end;
$$ language plpgsql security definer;

