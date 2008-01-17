
begin;

create table londiste.subscriber_pending_fkeys(
    from_table          text not null,
    to_table            text not null,
    fkey_name           text not null,
    fkey_def            text not null,
    
    primary key (from_table, fkey_name)
);

create table londiste.subscriber_pending_triggers (
    table_name          text not null,
    trigger_name        text not null,
    trigger_def         text not null,

    primary key (table_name, trigger_name)
);

-- drop function londiste.denytrigger();



create or replace function londiste.find_table_fkeys(i_table_name text)
returns setof londiste.subscriber_pending_fkeys as $$
declare
    fkey      record;
    tbl_oid   oid;
begin
    select londiste.find_table_oid(i_table_name) into tbl_oid;
        
    for fkey in
        select n1.nspname || '.' || t1.relname as from_table, n2.nspname || '.' || t2.relname as to_table,
            conname::text as fkey_name, 
            'alter table only ' || quote_ident(n1.nspname) || '.' || quote_ident(t1.relname)
            || ' add constraint ' || quote_ident(conname::text) || ' ' || pg_get_constraintdef(c.oid)
            as fkey_def
        from pg_constraint c, pg_namespace n1, pg_class t1, pg_namespace n2, pg_class t2
        where c.contype = 'f' and (c.conrelid = tbl_oid or c.confrelid = tbl_oid)
            and t1.oid = c.conrelid and n1.oid = t1.relnamespace
            and t2.oid = c.confrelid and n2.oid = t2.relnamespace
        order by 1,2,3
    loop
        return next fkey;
    end loop;
    
    return;
end;
$$ language plpgsql strict stable;





create or replace function londiste.find_table_triggers(i_table_name text)
returns setof londiste.subscriber_pending_triggers as $$
declare
    tg        record;
begin
    for tg in
        select n.nspname || '.' || c.relname as table_name, t.tgname::text as name, pg_get_triggerdef(t.oid) as def 
        from pg_trigger t, pg_class c, pg_namespace n
        where n.oid = c.relnamespace and c.oid = t.tgrelid
            and t.tgrelid = londiste.find_table_oid(i_table_name)
            and not t.tgisconstraint
    loop
        return next tg;
    end loop;
    
    return;
end;
$$ language plpgsql strict stable;


create or replace function londiste.find_column_types(tbl text)
returns text as $$
declare
    res      text;
    col      record;
    tbl_oid  oid;
begin
    tbl_oid := londiste.find_table_oid(tbl);
    res := '';
    for col in 
        SELECT CASE WHEN k.attname IS NOT NULL THEN 'k' ELSE 'v' END AS type
            FROM pg_attribute a LEFT JOIN (
                SELECT k.attname FROM pg_index i, pg_attribute k
                 WHERE i.indrelid = tbl_oid AND k.attrelid = i.indexrelid
                   AND i.indisprimary AND k.attnum > 0 AND NOT k.attisdropped
                ) k ON (k.attname = a.attname)
            WHERE a.attrelid = tbl_oid AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
    loop
        res := res || col.type;
    end loop;

    return res;
end;
$$ language plpgsql strict stable;





create or replace function londiste.subscriber_get_table_pending_fkeys(i_table_name text) 
returns setof londiste.subscriber_pending_fkeys as $$
declare
    fkeys   record;
begin
    for fkeys in
        select *
        from londiste.subscriber_pending_fkeys
        where from_table=i_table_name or to_table=i_table_name
        order by 1,2,3
    loop
        return next fkeys;
    end loop;
    
    return;
end;
$$ language plpgsql;


create or replace function londiste.subscriber_get_queue_valid_pending_fkeys(i_queue_name text)
returns setof londiste.subscriber_pending_fkeys as $$
declare
    fkeys   record;
begin
    for fkeys in
        select pf.*
        from londiste.subscriber_pending_fkeys pf
             left join londiste.subscriber_table st_from on (st_from.table_name = pf.from_table)
             left join londiste.subscriber_table st_to on (st_to.table_name = pf.to_table)
        where (st_from.table_name is null or (st_from.merge_state = 'ok' and st_from.snapshot is null))
          and (st_to.table_name is null or (st_to.merge_state = 'ok' and st_to.snapshot is null))
          and (coalesce(st_from.queue_name = i_queue_name, false)
               or coalesce(st_to.queue_name = i_queue_name, false))
        order by 1, 2, 3
    loop
        return next fkeys;
    end loop;
    
    return;
end;
$$ language plpgsql;


create or replace function londiste.subscriber_drop_table_fkey(i_from_table text, i_fkey_name text)
returns integer as $$
declare
    fkey       record;
begin        
    select * into fkey
    from londiste.find_table_fkeys(i_from_table) 
    where fkey_name = i_fkey_name and from_table = i_from_table;
    
    if not found then
        return 0;
    end if;
            
    insert into londiste.subscriber_pending_fkeys values (fkey.from_table, fkey.to_table, i_fkey_name, fkey.fkey_def);
        
    execute 'alter table only ' || londiste.quote_fqname(fkey.from_table)
            || ' drop constraint ' || quote_ident(i_fkey_name);
    
    return 1;
end;
$$ language plpgsql;


create or replace function londiste.subscriber_restore_table_fkey(i_from_table text, i_fkey_name text)
returns integer as $$
declare
    fkey    record;
begin
    select * into fkey
    from londiste.subscriber_pending_fkeys 
    where fkey_name = i_fkey_name and from_table = i_from_table;
    
    if not found then
        return 0;
    end if;
    
    delete from londiste.subscriber_pending_fkeys where fkey_name = fkey.fkey_name;
        
    execute fkey.fkey_def;
        
    return 1;
end;
$$ language plpgsql;



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
    
    execute 'drop trigger ' || i_trigger_name || ' on ' || i_table_name;
    
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





create or replace function londiste.quote_fqname(i_name text)
returns text as $$
declare
    res     text;
    pos     integer;
    s       text;
    n       text;
begin
    pos := position('.' in i_name);
    if pos > 0 then
        s := substring(i_name for pos - 1);
        n := substring(i_name from pos + 1);
    else
        s := 'public';
        n := i_name;
    end if;
    return quote_ident(s) || '.' || quote_ident(n);
end;
$$ language plpgsql strict immutable;




create or replace function londiste.find_rel_oid(tbl text, kind text)
returns oid as $$
declare
    res      oid;
    pos      integer;
    schema   text;
    name     text;
begin
    pos := position('.' in tbl);
    if pos > 0 then
        schema := substring(tbl for pos - 1);
        name := substring(tbl from pos + 1);
    else
        schema := 'public';
        name := tbl;
    end if;
    select c.oid into res
      from pg_namespace n, pg_class c
     where c.relnamespace = n.oid
       and c.relkind = kind
       and n.nspname = schema and c.relname = name;
    if not found then
        if kind = 'r' then
            raise exception 'table not found';
        elsif kind = 'S' then
            raise exception 'seq not found';
        else
            raise exception 'weird relkind';
        end if;
    end if;

    return res;
end;
$$ language plpgsql strict stable;

create or replace function londiste.find_table_oid(tbl text)
returns oid as $$
begin
    return londiste.find_rel_oid(tbl, 'r');
end;
$$ language plpgsql strict stable;

create or replace function londiste.find_seq_oid(tbl text)
returns oid as $$
begin
    return londiste.find_rel_oid(tbl, 'S');
end;
$$ language plpgsql strict stable;




create or replace function londiste.get_last_tick(i_consumer text)
returns bigint as $$
declare
    res   bigint;
begin
    select last_tick_id into res
      from londiste.completed
     where consumer_id = i_consumer;
    return res;
end;
$$ language plpgsql security definer strict stable;



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

    execute 'create trigger ' || tgname
        || ' after insert or update or delete on '
        || i_table_name
        || ' for each row execute procedure pgq.logtriga('
        || quote_literal(i_queue_name) || ', '
        || quote_literal(i_col_types) || ', '
        || quote_literal(i_table_name) || ')';

    return 1;
end;
$$ language plpgsql security definer;




create or replace function londiste.provider_notify_change(i_queue_name text)
returns integer as $$
declare
    res      text;
    tbl      record;
begin
    res := '';
    for tbl in
        select table_name from londiste.provider_table
            where queue_name = i_queue_name
            order by nr
    loop
        if res = '' then
            res := tbl.table_name;
        else
            res := res || ',' || tbl.table_name;
        end if;
    end loop;
    
    perform pgq.insert_event(i_queue_name, 'T', res);

    return 1;
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
        execute 'drop trigger ' || tgname || ' on ' || i_table_name;
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





create or replace function londiste.set_last_tick(
    i_consumer text,
    i_tick_id bigint)
returns integer as $$
begin
    if i_tick_id is null then
        delete from londiste.completed
         where consumer_id = i_consumer;
    else
        update londiste.completed
           set last_tick_id = i_tick_id
         where consumer_id = i_consumer;
        if not found then
            insert into londiste.completed (consumer_id, last_tick_id)
                values (i_consumer, i_tick_id);
        end if;
    end if;

    return 1;
end;
$$ language plpgsql security definer;




create or replace function londiste.subscriber_remove_table(
    i_queue_name text, i_table text)
returns integer as $$
declare
    link  text;
begin
    delete from londiste.subscriber_table
     where queue_name = i_queue_name
       and table_name = i_table;
    if not found then
        raise exception 'no such table';
    end if;

    -- sync link
    link := londiste.link_dest(i_queue_name);
    if link is not null then
        delete from londiste.provider_table
            where queue_name = link
              and table_name = i_table;
        perform londiste.provider_notify_change(link);
    end if;

    return 0;
end;
$$ language plpgsql security definer;





grant usage on schema londiste to public;
grant select on londiste.provider_table to public;
grant select on londiste.completed to public;
grant select on londiste.link to public;
grant select on londiste.subscriber_table to public;



end;


