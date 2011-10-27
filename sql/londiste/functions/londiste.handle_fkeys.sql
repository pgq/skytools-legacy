
create or replace function londiste.get_table_pending_fkeys(i_table_name text) 
returns setof londiste.pending_fkeys as $$
-- ----------------------------------------------------------------------
-- Function: londiste.get_table_pending_fkeys(1)
--
--      Return dropped fkeys for table.
--
-- Parameters:
--      i_table_name - fqname
--
-- Returns:
--      desc
-- ----------------------------------------------------------------------
declare
    fkeys   record;
begin
    for fkeys in
        select *
        from londiste.pending_fkeys
        where from_table = i_table_name or to_table = i_table_name
        order by 1,2,3
    loop
        return next fkeys;
    end loop;
    return;
end;
$$ language plpgsql strict stable;


create or replace function londiste.get_valid_pending_fkeys(i_queue_name text)
returns setof londiste.pending_fkeys as $$
-- ----------------------------------------------------------------------
-- Function: londiste.get_valid_pending_fkeys(1)
--
--      Returns dropped fkeys where both sides are in sync now.
--
-- Parameters:
--      i_queue_name - cascaded queue name
--
-- Returns:
--      desc
-- ----------------------------------------------------------------------
declare
    fkeys   record;
begin
    for fkeys in
        select pf.*
        from londiste.pending_fkeys pf
        order by 1, 2, 3
    loop
        perform 1
           from londiste.table_info st_from
          where coalesce(st_from.dest_table, st_from.table_name) = fkeys.from_table
            and st_from.merge_state = 'ok'
            and st_from.custom_snapshot is null
            and st_from.queue_name = i_queue_name;
        if not found then
            continue;
        end if;
        perform 1
           from londiste.table_info st_to
          where coalesce(st_to.dest_table, st_to.table_name) = fkeys.to_table
            and st_to.merge_state = 'ok'
            and st_to.custom_snapshot is null
            and st_to.queue_name = i_queue_name;
        if not found then
            continue;
        end if;
        return next fkeys;
    end loop;
    
    return;
end;
$$ language plpgsql strict stable;


create or replace function londiste.drop_table_fkey(i_from_table text, i_fkey_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.drop_table_fkey(2)
--
--      Drop one fkey, save in pending table.
-- ----------------------------------------------------------------------
declare
    fkey       record;
begin        
    select * into fkey
    from londiste.find_table_fkeys(i_from_table) 
    where fkey_name = i_fkey_name and from_table = i_from_table;
    
    if not found then
        return 0;
    end if;
            
    insert into londiste.pending_fkeys values (fkey.from_table, fkey.to_table, i_fkey_name, fkey.fkey_def);
        
    execute 'alter table only ' || londiste.quote_fqname(fkey.from_table)
            || ' drop constraint ' || quote_ident(i_fkey_name);
    
    return 1;
end;
$$ language plpgsql strict;


create or replace function londiste.restore_table_fkey(i_from_table text, i_fkey_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.restore_table_fkey(2)
--
--      Restore dropped fkey.
--
-- Parameters:
--      i_from_table - source table
--      i_fkey_name  - fkey name
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    fkey    record;
begin
    select * into fkey
    from londiste.pending_fkeys 
    where fkey_name = i_fkey_name and from_table = i_from_table;
    
    if not found then
        return 0;
    end if;

    execute fkey.fkey_def;

    delete from londiste.pending_fkeys where fkey_name = fkey.fkey_name;
        
    return 1;
end;
$$ language plpgsql strict;

