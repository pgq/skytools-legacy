
create or replace function londiste.provider_create_trigger(
    i_queue_name    text,
    i_table_name    text,
    i_col_types     text
) returns integer strict as $$
declare
    tgname text;
    sql    text;
begin
    select trigger_name into tgname
        from londiste.provider_table
        where queue_name = i_queue_name
          and table_name = i_table_name;
    if not found then
        raise exception 'table not found';
    end if;

    sql := 'select pgq.insert_event('
        || quote_literal(i_queue_name)
        || ', $1, $2, '
        || quote_literal(i_table_name)
        || ', NULL, NULL, NULL)';
    execute 'create trigger ' || tgname
        || ' after insert or update or delete on '
        || i_table_name
        || ' for each row execute procedure logtriga($arg1$'
        || i_col_types || '$arg1$, $arg2$' || sql || '$arg2$)';

    return 1;
end;
$$ language plpgsql security definer;

