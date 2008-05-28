
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


