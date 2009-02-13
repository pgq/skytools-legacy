
create or replace function londiste.global_update_seq(
    in i_queue_name text, in i_seq_name text, in i_value int8,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.global_update_seq(3)
--
--      Update seq.
--
-- Parameters:
--      i_queue_name  - set name
--      i_seq_name  - seq name
--      i_value     - new published value
--
-- Returns:
--      200 - OK
-- ----------------------------------------------------------------------
declare
    n record;
    fqname text;
    seq record;
begin
    select node_type, node_name into n
        from pgq_node.node_info
        where queue_name = i_queue_name;
    if not found then
        select 404, 'Set not found: ' || i_queue_name into ret_code, ret_note;
        return;
    end if;
    if n.node_type = 'root' then
        select 402, 'Must not run on root node' into ret_code, ret_note;
        return;
    end if;

    fqname := londiste.make_fqname(i_seq_name);
    select last_value, local from londiste.seq_info
        into seq
        where queue_name = i_queue_name and seq_name = fqname
        for update;
    if not found then
        insert into londiste.seq_info
            (queue_name, seq_name, last_value)
        values (i_queue_name, fqname, i_value);
    else
        update londiste.seq_info
            set last_value = i_value
            where queue_name = i_queue_name and seq_name = fqname;
        if seq.local then
            perform pgq.seq_setval(fqname, i_value);
        end if;
    end if;
    select 200, 'Sequence updated' into ret_code, ret_note;
    return;
end;
$$ language plpgsql;

