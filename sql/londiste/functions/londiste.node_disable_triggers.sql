
create or replace function londiste.node_disable_triggers(
    in i_set_name   text,
    in i_table_name text,
    out ret_code    int4,
    out ret_note    text)
returns setof record strict as $$
-- ----------------------------------------------------------------------
-- Function: londiste.node_disable_triggers(2)
--
--      Drop all registered triggers from particular table.
-- ----------------------------------------------------------------------
declare
    tbl_oid oid;
    fq_table_name text;
    tg record;
    is_active int4;
begin
    fq_table_name := londiste.make_fqname(i_table_name);
    perform 1 from pgq_set.set_info where set_name = i_set_name;
    if not found then
        select 400, 'Unknown set: ' || i_set_name;
        return next;
        return;
    end if;
    tbl_oid := londiste.find_table_oid(fq_table_name);
    for tg in
        select tg_name, tg_type, tg_def from londiste.node_trigger
         where set_name = i_set_name and table_name = fq_table_name
         order by tg_name
    loop
        -- check if active
        perform 1 from pg_catalog.pg_trigger
         where tgrelid = tbl_oid
           and tgname = tg.tg_name;
        if found then
            execute 'drop trigger ' || quote_ident(tg.tg_name)
                || ' on ' || londiste.quote_fqname(fq_table_name);
            select 200, 'Dropped trigger ' || tg.tg_name
                || ' from table ' || fq_table_name
                into ret_code, ret_note;
                return next;
        end if;
    end loop;
    return;
end;
$$ language plpgsql security definer;

create or replace function londiste.node_disable_triggers(
    in i_set_name   text,
    out ret_code    int4,
    out ret_note    text)
returns setof record strict as $$
-- ----------------------------------------------------------------------
-- Function: londiste.node_disable_triggers(1)
--
--      Drop all registered triggers from set tables.
-- ----------------------------------------------------------------------
declare
    t record;
begin
    for t in
        select table_name from londiste.node_table
         where set_name = i_set_name
         order by nr
    loop
        for ret_code, ret_note in
            select f.ret_code, f.ret_note
                from londiste.node_disable_triggers(i_set_name, t.table_name) f
        loop
            return next;
        end loop;
    end loop;
    return;
end;
$$ language plpgsql security definer;

