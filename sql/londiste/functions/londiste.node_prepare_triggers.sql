
create or replace function londiste.node_prepare_triggers(
    in i_set_name   text,
    in i_table_name text,
    out ret_code    int4,
    out ret_desc    text)
returns setof record strict as $$
-- ----------------------------------------------------------------------
-- Function: londiste.node_prepare_triggers(2)
--
--      Regsiter Londiste trigger for table.
-- ----------------------------------------------------------------------
declare
    t_name   text;
    logtrg   text;
    denytrg  text;
    logtrg_name text;
    denytrg_name text;
    qname    text;
    fq_table_name text;
begin
    fq_table_name := londiste.make_fqname(i_table_name);
    select queue_name into qname from pgq_set.set_info where set_name = i_set_name;
    if not found then
        select 400, 'Set not found: ' || i_set_name into ret_code, ret_desc;
        return next;
        return;
    end if;
    logtrg_name := i_set_name || '_logtrigger';
    denytrg_name := i_set_name || '_denytrigger';
    logtrg := 'create trigger ' || quote_ident(logtrg_name)
        || ' after insert or update or delete on ' || londiste.quote_fqname(fq_table_name)
        || ' for each row execute procedure pgq.sqltriga(' || quote_literal(qname) || ')';
    insert into londiste.node_trigger (set_name, table_name, tg_name, tg_type, tg_def)
    values (i_set_name, fq_table_name, logtrg_name, 'root', logtrg);
    select 200, logtrg into ret_code, ret_desc;
    return next;

    denytrg := 'create trigger ' || quote_ident(denytrg_name)
        || ' before insert or update or delete on ' || londiste.quote_fqname(fq_table_name)
        || ' for each row execute procedure pgq.denytriga(' || quote_literal(qname) || ')';
    insert into londiste.node_trigger (set_name, table_name, tg_name, tg_type, tg_def)
    values (i_set_name, fq_table_name, denytrg_name, 'non-root', denytrg);
    select 200, denytrg into ret_code, ret_desc;
    return next;

    return;
end;
$$ language plpgsql security definer;

