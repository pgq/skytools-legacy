
create or replace function londiste.provider_remove_table(
    i_queue_name   text,
    i_table_name   text
) returns integer as $$
declare
    tgname text;
begin
    if londiste.link_source(i_queue_name) is not null then
        raise exception 'Linked queue, manipulation not allowed';
    end if;

    select trigger_name into tgname from londiste.provider_table
        where queue_name = i_queue_name
          and table_name = i_table_name;
    if not found then
        raise exception 'no such table registered';
    end if;

    begin
        execute 'drop trigger ' || quote_ident(tgname) || ' on ' || londiste.quote_fqname(i_table_name);
    exception
        when undefined_table then
            raise notice 'table % does not exist', i_table_name;
        when undefined_object then
            raise notice 'trigger % does not exist on table %', tgname, i_table_name;
    end;

    delete from londiste.provider_table
        where queue_name = i_queue_name
          and table_name = i_table_name;

    return 1;
end;
$$ language plpgsql security definer;


