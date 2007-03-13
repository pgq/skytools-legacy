
create or replace function londiste.provider_get_seq_list(i_queue_name text)
returns setof text as $$
declare
    rec record;
begin
    for rec in
        select seq_name from londiste.provider_seq
            where queue_name = i_queue_name
            order by nr
    loop
        return next rec.seq_name;
    end loop;
    return;
end;
$$ language plpgsql security definer;

