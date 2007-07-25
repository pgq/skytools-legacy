create or replace function londiste.provider_add_table(
    i_queue_name    text,
    i_table_name    text,
    i_col_types     text
) returns integer strict as $$
declare
    tgname text;
    sql    text;
begin
    if londiste.link_source(i_queue_name) is not null then
        raise exception 'Linked queue, manipulation not allowed';
    end if;

    if position('k' in i_col_types) < 1 then
        raise exception 'need key column';
    end if;
    if position('.' in i_table_name) < 1 then
        raise exception 'need fully-qualified table name';
    end if;
    select queue_name into tgname
        from pgq.queue where queue_name = i_queue_name;
    if not found then
        raise exception 'no such event queue';
    end if;

    tgname := i_queue_name || '_logger';
    tgname := replace(lower(tgname), '.', '_');
    insert into londiste.provider_table
        (queue_name, table_name, trigger_name)
        values (i_queue_name, i_table_name, tgname);

    perform londiste.provider_create_trigger(
        i_queue_name, i_table_name, i_col_types);

    return 1;
end;
$$ language plpgsql security definer;

create or replace function londiste.provider_add_table(
    i_queue_name text,
    i_table_name text
) returns integer as $$
begin
    return londiste.provider_add_table(i_queue_name, i_table_name,
        londiste.find_column_types(i_table_name));
end;
$$ language plpgsql security definer;

