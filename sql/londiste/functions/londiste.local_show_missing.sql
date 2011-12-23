
create or replace function londiste.local_show_missing(
    in i_queue_name text,
    out obj_kind text, out obj_name text)
returns setof record as $$ 
-- ----------------------------------------------------------------------
-- Function: londiste.local_show_missing(1)
--
--      Return info about missing tables.  On root show tables
--      not registered on set, on branch/leaf show tables
--      in set but not registered locally.
-- ----------------------------------------------------------------------
begin
    if pgq_node.is_root_node(i_queue_name) then
        for obj_kind, obj_name in
            select r.relkind, n.nspname || '.' || r.relname
                from pg_catalog.pg_class r, pg_catalog.pg_namespace n
                where n.oid = r.relnamespace
                  and r.relkind in ('r', 'S')
                  and n.nspname not in ('pgq', 'pgq_ext', 'pgq_node', 'londiste', 'pg_catalog', 'information_schema')
                  and n.nspname !~ '^pg_(toast|temp)'
                  and not exists (select 1 from londiste.table_info
                                   where queue_name = i_queue_name and local
                                     and coalesce(dest_table, table_name) = (n.nspname || '.' || r.relname))
                order by 1, 2
        loop
            return next;
        end loop;
    else
        for obj_kind, obj_name in
            select 'S', s.seq_name from londiste.seq_info s
                where s.queue_name = i_queue_name
                  and not s.local
            union all
            select 'r', t.table_name from londiste.table_info t
                where t.queue_name = i_queue_name
                  and not t.local
            order by 1, 2
        loop
            return next;
        end loop;
    end if;
    return;
end; 
$$ language plpgsql strict stable;

