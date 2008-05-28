
begin;



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





create or replace function londiste.subscriber_get_table_pending_triggers(i_table_name text)
returns setof londiste.subscriber_pending_triggers as $$
declare
    trigger    record;
begin
    for trigger in
        select *
        from londiste.subscriber_pending_triggers
        where table_name = i_table_name
    loop
        return next trigger;
    end loop;
    
    return;
end;
$$ language plpgsql strict stable;


create or replace function londiste.subscriber_drop_table_trigger(i_table_name text, i_trigger_name text)
returns integer as $$
declare
    trig_def record;
begin
    select * into trig_def
    from londiste.find_table_triggers(i_table_name)
    where trigger_name = i_trigger_name;
    
    if FOUND is not true then
        return 0;
    end if;
    
    insert into londiste.subscriber_pending_triggers(table_name, trigger_name, trigger_def) 
        values (i_table_name, i_trigger_name, trig_def.trigger_def);
    
    execute 'drop trigger ' || quote_ident(i_trigger_name)
        || ' on ' || londiste.quote_fqname(i_table_name);
    
    return 1;
end;
$$ language plpgsql;


create or replace function londiste.subscriber_drop_all_table_triggers(i_table_name text)
returns integer as $$
declare
    trigger record;
begin
    for trigger in
        select trigger_name as name
        from londiste.find_table_triggers(i_table_name)
    loop
        perform londiste.subscriber_drop_table_trigger(i_table_name, trigger.name);
    end loop;
    
    return 1;
end;
$$ language plpgsql;


create or replace function londiste.subscriber_restore_table_trigger(i_table_name text, i_trigger_name text)
returns integer as $$
declare
    trig_def text;
begin
    select trigger_def into trig_def
    from londiste.subscriber_pending_triggers
    where (table_name, trigger_name) = (i_table_name, i_trigger_name);
    
    if not found then
        return 0;
    end if;
    
    delete from londiste.subscriber_pending_triggers 
    where table_name = i_table_name and trigger_name = i_trigger_name;
    
    execute trig_def;

    return 1;
end;
$$ language plpgsql;


create or replace function londiste.subscriber_restore_all_table_triggers(i_table_name text)
returns integer as $$
declare
    trigger record;
begin
    for trigger in
        select trigger_name as name
        from londiste.subscriber_get_table_pending_triggers(i_table_name)
    loop
        perform londiste.subscriber_restore_table_trigger(i_table_name, trigger.name);
    end loop;
    
    return 1;
end;
$$ language plpgsql;





create or replace function londiste.version()
returns text as $$
begin
    return '2.1.7';
end;
$$ language plpgsql;



end;


