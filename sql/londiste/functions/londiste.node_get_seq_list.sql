
create or replace function londiste.provider_get_seq_list(i_set_name text)
returns setof text as $$
-- ----------------------------------------------------------------------
-- Function: londiste.node_get_seq_list(x)
--
--      Returns registered seqs on this Londiste node.
-- ----------------------------------------------------------------------
declare
    rec record;
begin
    for rec in
        select seq_name from londiste.node_seq
            where set_name = i_set_name
            order by nr
    loop
        return next rec.seq_name;
    end loop;
    return;
end;
$$ language plpgsql strict;

