
create or replace function londiste.local_remove_seq(
    in i_queue_name text, in i_seq_name text,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.local_remove_seq(2)
--
--      Remove sequence.
--
-- Parameters:
--      i_queue_name      - set name
--      i_seq_name      - sequence name
--
-- Returns:
--      200 - OK
--      404 - Sequence not found
-- ----------------------------------------------------------------------
declare
    fqname text;
begin
    fqname := londiste.make_fqname(i_seq_name);
    if pgq_node.is_root_node(i_queue_name) then
        select f.ret_code, f.ret_note
            into ret_code, ret_note
            from londiste.global_remove_seq(i_queue_name, fqname) f;
        return;
    end if;
    update londiste.seq_info
        set local = false
        where queue_name = i_queue_name
          and seq_name = fqname
          and local;
    if not found then
        select 404, 'Sequence not found: '||fqname into ret_code, ret_note;
        return;
    end if;

    select 200, 'Sequence removed: '||fqname into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;

