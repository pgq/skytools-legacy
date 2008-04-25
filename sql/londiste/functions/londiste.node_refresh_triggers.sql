
create or replace function londiste.node_refresh_triggers(
    in i_set_name   text,
    in i_table_name text,
    out ret_code    int4,
    out ret_note    text)
returns setof record strict as $$
-- ----------------------------------------------------------------------
-- Function: londiste.node_refresh_triggers(2)
--
--      Sync actual trigger state with registered triggers.
-- ----------------------------------------------------------------------
declare
    tbl_oid oid;
    fq_table_name text;
    tg record;
    is_root bool;
    is_active int4;
begin
    fq_table_name := londiste.make_fqname(i_table_name);
    perform 1 from pgq_set.set_info where set_name = i_set_name;
    if not found then
        select 400, 'Unknown set: ' || i_set_name;
        return next;
        return;
    end if;
    is_root := pgq_set.is_root(i_set_name);
    tbl_oid := londiste.find_table_oid(fq_table_name);
    for tg in
        select tg_name, tg_type, tg_def from londiste.node_trigger
         where set_name = i_set_name and table_name = fq_table_name
         order by tg_name
    loop
        if tg.tg_type not in ('root', 'non-root') then
            select 400, 'trigger ' || tg.tg_name
                || ' on table ' || fq_table_name
                || ' had unsupported type: ' || tg.tg_type
                into ret_code, ret_note;
            return next;
        else
            -- check if active
            select count(1) into is_active
              from pg_catalog.pg_trigger
             where tgrelid = tbl_oid
               and tgname = tg.tg_name;

            -- create or drop if needed
            if (tg.tg_type = 'root') = is_root then
                -- trigger must be active
                if is_active = 0 then
                    execute tg.tg_def;
                    select 200, 'Created trigger ' || tg.tg_name
                        || ' on table ' || fq_table_name
                        into ret_code, ret_note;
                    return next;
                end if;
            else
                -- trigger must be dropped
                if is_active = 1 then
                    execute 'drop trigger ' || quote_ident(tg.tg_name)
                        || ' on ' || londiste.quote_fqname(fq_table_name);
                    select 200, 'Dropped trigger ' || tg.tg_name
                        || ' from table ' || fq_table_name
                        into ret_code, ret_note;
                    return next;
                end if;
            end if;
        end if;
    end loop;
    return;
end;
$$ language plpgsql security definer;

create or replace function londiste.node_refresh_triggers(
    in i_set_name   text,
    out ret_code    int4,
    out ret_note    text)
returns setof record strict as $$
-- ----------------------------------------------------------------------
-- Function: londiste.node_refresh_triggers(2)
--
--      Sync actual trigger state with registered triggers for all tables.
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
                from londiste.node_refresh_triggers(i_set_name, t.table_name) f
        loop
            return next;
        end loop;
    end loop;
    return;
end;
$$ language plpgsql security definer;

