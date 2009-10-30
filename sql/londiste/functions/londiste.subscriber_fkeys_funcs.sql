

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
        select pf.* from londiste.subscriber_pending_fkeys pf
            join londiste.subscriber_table st_from 
                on (st_from.table_name = pf.from_table and st_from.merge_state = 'ok' and st_from.snapshot is null)
            join londiste.subscriber_table st_to   
                on (st_to.table_name = pf.to_table and st_to.merge_state = 'ok' and st_to.snapshot is null)
            -- change the AND to OR to allow fkeys between tables coming from different queues
            where (st_from.queue_name = i_queue_name and st_to.queue_name = i_queue_name)
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
