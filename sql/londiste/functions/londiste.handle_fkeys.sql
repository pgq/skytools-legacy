
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


create or replace function londiste.node_get_valid_pending_fkeys(i_set_name text)
returns setof londiste.pending_fkeys as $$
-- ----------------------------------------------------------------------
-- Function: londiste.node_get_valid_pending_fkeys(1)
--
--      Returns dropped fkeys where both sides are in sync now.
--
-- Parameters:
--      i_set_name - sets name
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
             left join londiste.node_table st_from on (st_from.table_name = pf.from_table)
             left join londiste.node_table st_to on (st_to.table_name = pf.to_table)
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
$$ language plpgsql strict stable;


create or replace function londiste.drop_table_fkey(i_from_table text, i_fkey_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.drop_table_fkey(x)
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

