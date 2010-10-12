
create or replace function londiste.root_check_seqs(
    in i_queue_name text, in i_buffer int8,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.root_check_seqs(1)
--
--      Check sequences, and publish values if needed.
--
-- Parameters:
--      i_queue_name    - set name
--      i_buffer        - safety room
--
-- Returns:
--      200 - OK
--      402 - Not a root node
--      404 - Queue not found
-- ----------------------------------------------------------------------
declare
    n record;
    seq record;
    real_value int8;
    pub_value int8;
    real_buffer int8;
begin
    if i_buffer is null or i_buffer < 10 then
        real_buffer := 10000;
    else
        real_buffer := i_buffer;
    end if;

    select node_type, node_name into n
        from pgq_node.node_info
        where queue_name = i_queue_name
        for update;
    if not found then
        select 404, 'Queue not found: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;
    if n.node_type <> 'root' then
        select 402, 'Not a root node' into ret_code, ret_note;
        return;
    end if;

    for seq in
        select seq_name, last_value,
               londiste.quote_fqname(seq_name) as fqname
            from londiste.seq_info
            where queue_name = i_queue_name
                and local
            order by nr
    loop
        execute 'select last_value from ' || seq.fqname into real_value;
        if real_value + real_buffer >= seq.last_value then
            pub_value := real_value + real_buffer * 3;
            perform pgq.insert_event(i_queue_name, 'londiste.update-seq',
                        pub_value::text, seq.seq_name, null, null, null);
            update londiste.seq_info set last_value = pub_value
                where queue_name = i_queue_name
                    and seq_name = seq.seq_name;
        end if;
    end loop;

    select 100, 'Sequences updated' into ret_code, ret_note;
    return;
end;
$$ language plpgsql;

create or replace function londiste.root_check_seqs(
    in i_queue_name text,
    out ret_code int4, out ret_note text)
as $$
begin
    select f.ret_code, f.ret_note
        into ret_code, ret_note
        from londiste.root_check_seqs(i_queue_name, 10000) f;
    return;
end;
$$ language plpgsql;

