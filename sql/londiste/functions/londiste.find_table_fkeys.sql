
create or replace function londiste.find_table_fkeys(i_table_name text)
returns setof londiste.pending_fkeys as $$
-- ----------------------------------------------------------------------
-- Function: londiste.find_table_fkeys(1)
--
--      Return all active fkeys.
--
-- Parameters:
--      i_table_name    - fqname
--
-- Returns:
--      from_table      - fqname
--      to_table        - fqname
--      fkey_name       - name
--      fkey_def        - full def
-- ----------------------------------------------------------------------
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


