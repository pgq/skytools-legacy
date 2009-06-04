
create or replace function londiste.local_add_seq(
    in i_queue_name text, in i_seq_name text,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_add_seq(2)
--
--      Register sequence.
--
-- Parameters:
--      i_queue_name    - cascaded queue name
--      i_seq_name      - seq name
--
-- Returns:
--      200 - OK
--      400 - Not found
-- ----------------------------------------------------------------------
declare
    fq_seq_name text;
    lastval int8;
    seq record;
begin
    fq_seq_name := londiste.make_fqname(i_seq_name);

    perform 1 from pg_class
        where oid = londiste.find_seq_oid(fq_seq_name);
    if not found then
        select 400, 'Sequence not found: ' || fq_seq_name into ret_code, ret_note;
        return;
    end if;

    if pgq_node.is_root_node(i_queue_name) then
        select local, last_value into seq
            from londiste.seq_info
            where queue_name = i_queue_name
                and seq_name = fq_seq_name
            for update;
        if found and seq.local then
            select 201, 'Sequence already added: ' || fq_seq_name
                into ret_code, ret_note;
            return;
        end if;
        if not seq.local then
            update londiste.seq_info set local = true
                where queue_name = i_queue_name and seq_name = fq_seq_name;
        else
            insert into londiste.seq_info (queue_name, seq_name, local, last_value)
                values (i_queue_name, fq_seq_name, true, 0);
        end if;
        perform * from londiste.root_check_seqs(i_queue_name);
    else
        select local, last_value into seq
            from londiste.seq_info
            where queue_name = i_queue_name
                and seq_name = fq_seq_name
            for update;
        if not found then
            select 404, 'Unknown sequence: ' || fq_seq_name
                into ret_code, ret_note;
            return;
        end if;
        if seq.local then
            select 201, 'Sequence already added: ' || fq_seq_name
                into ret_code, ret_note;
            return;
        end if;
        update londiste.seq_info set local = true
            where queue_name = i_queue_name and seq_name = fq_seq_name;
        perform pgq.seq_setval(fq_seq_name, seq.last_value);
    end if;

    select 200, 'Sequence added: ' || fq_seq_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql;

