
create or replace function londiste.get_pending_triggers(i_table_name text)
returns setof londiste.pending_triggers as $$
-- ----------------------------------------------------------------------
-- Function: londiste.get_pending_triggers(1)
--
--      Returns dropped triggers for one table.
--
-- Parameters:
--      i_table_name - fqname
--
-- Returns:
--      list of triggers
-- ----------------------------------------------------------------------
declare
    trigger    record;
begin
    for trigger in
        select *
        from londiste.pending_triggers
        where table_name = i_table_name
    loop
        return next trigger;
    end loop;
    
    return;
end;
$$ language plpgsql strict stable;


create or replace function londiste.drop_table_trigger(i_table_name text, i_trigger_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.drop_table_trigger(2)
--
--      Drop one trigger, saves it to pending table.
-- ----------------------------------------------------------------------
declare
    trig_def record;
begin
    select * into trig_def
    from londiste.find_table_triggers(i_table_name)
    where trigger_name = i_trigger_name;
    
    if FOUND is not true then
        return 0;
    end if;
    
    insert into londiste.pending_triggers(table_name, trigger_name, trigger_def) 
        values (i_table_name, i_trigger_name, trig_def.trigger_def);
    
    execute 'drop trigger ' || i_trigger_name || ' on ' || i_table_name;
    
    return 1;
end;
$$ language plpgsql;


create or replace function londiste.drop_all_table_triggers(i_table_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.drop_all_table_triggers(1)
--
--      Drop all triggers that exist.
-- ----------------------------------------------------------------------
declare
    trigger record;
begin
    for trigger in
        select trigger_name as name
        from londiste.find_table_triggers(i_table_name)
    loop
        perform londiste.drop_table_trigger(i_table_name, trigger.name);
    end loop;
    
    return 1;
end;
$$ language plpgsql;


create or replace function londiste.restore_table_trigger(i_table_name text, i_trigger_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.restore_table_trigger(2)
--
--      Restore one trigger.
-- ----------------------------------------------------------------------
declare
    trig_def text;
begin
    select trigger_def into trig_def
    from londiste.pending_triggers
    where (table_name, trigger_name) = (i_table_name, i_trigger_name);
    
    if not found then
        return 0;
    end if;
    
    delete from londiste.pending_triggers 
    where table_name = i_table_name and trigger_name = i_trigger_name;
    
    execute trig_def;

    return 1;
end;
$$ language plpgsql;


create or replace function londiste.restore_all_table_triggers(i_table_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.restore_all_table_triggers(1)
--
--      Restore all dropped triggers.
-- ----------------------------------------------------------------------
declare
    trigger record;
begin
    for trigger in
        select trigger_name as name
        from londiste.get_pending_triggers(i_table_name)
    loop
        perform londiste.restore_table_trigger(i_table_name, trigger.name);
    end loop;
    
    return 1;
end;
$$ language plpgsql;


