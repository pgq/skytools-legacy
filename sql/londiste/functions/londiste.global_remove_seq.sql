
create or replace function londiste.global_remove_seq(
    in i_queue_name text, in i_seq_name text,
    out ret_code int4, out ret_note text)
as $$
-- ----------------------------------------------------------------------
-- Function: londiste.global_remove_seq(2)
--
--      Removes sequence registration in set.
--
-- Called by:
--      - On root by londiste.local_remove_seq()
--      - Elsewhere by consumer receiving seq remove event
--
-- Returns:
--      200 - OK
--      400 - not found
-- ----------------------------------------------------------------------
declare
    fq_name text;
begin
    fq_name := londiste.make_fqname(i_seq_name);
    delete from londiste.seq_info
        where queue_name = i_queue_name
          and seq_name = fq_name;
    if not found then
        select 400, 'Sequence not found: '||fq_name into ret_code, ret_note;
        return;
    end if;
    if pgq_node.is_root_node(i_queue_name) then
        perform londiste.root_notify_change(i_queue_name, 'londiste.remove-seq', fq_name);
    end if;
    select 200, 'Sequence removed: '||fq_name into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;

