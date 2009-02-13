
create or replace function londiste.get_seq_list(
    in i_queue_name text,
    out seq_name text,
    out last_value int8,
    out local boolean)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: londiste.get_seq_list(1)
--
--      Returns registered seqs on this Londiste node.
--
-- Result fiels:
--      seq_name    - fully qualified name of sequence
--      last_value  - last globally published value
--      local       - is locally registered
-- ----------------------------------------------------------------------
declare
    rec record;
begin
    for seq_name, last_value, local in
        select s.seq_name, s.last_value, s.local from londiste.seq_info s
            where s.queue_name = i_queue_name
            order by s.nr, s.seq_name
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql strict;

