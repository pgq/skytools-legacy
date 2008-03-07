
create or replace function londiste.node_add_seq(
    in i_set_name text, in i_seq_name text,
    out ret_code int4, out ret_text text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.node_add_seq(2)
--
--      Register sequence.
--
-- Parameters:
--      i_set_name  - set name
--      i_seq_name  - seq name
--
-- Returns:
--      200 - OK
--      400 - Not found
-- ----------------------------------------------------------------------
declare
    fq_seq_name text;
begin
    fq_seq_name := londiste.make_fqname(i_seq_name);

    perform 1 from pg_class
        where oid = londiste.find_seq_oid(fq_seq_name);
    if not found then
        select 400, 'Sequence not found: ' || fq_seq_name into ret_code, ret_text;
        return;
    end if;

    perform 1 from londiste.node_seq
        where set_name = i_set_name and seq_name = fq_seq_name;
    if found then
        select 200, 'OK, seqence already added' into ret_code, ret_text;
        return;
    end if;

    if pgq_set.is_root(i_set_name) then
        insert into londiste.set_seq (set_name, seq_name)
            values (i_set_name, fq_seq_name);
        perform londiste.node_notify_change(i_set_name, 'add-seq', fq_seq_name);
    end if;

    insert into londiste.node_seq (set_name, seq_name)
        values (i_set_name, fq_seq_name);

    select 200, 'OK' into ret_code, ret_text;
    return;
end;
$$ language plpgsql;

