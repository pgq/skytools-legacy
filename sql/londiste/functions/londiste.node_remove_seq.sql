
create or replace function londiste.provider_remove_seq(
    in i_set_name text, in i_seq_name text,
    out ret_code int4, out ret_note text)
as $$
begin
    delete from londiste.node_seq
        where set_name = i_set_name
          and seq_name = i_seq_name;
    if not found then
        select 400, 'Not found: '||i_seq_name into ret_code, ret_note;
        return;
    end if;

    -- perform londiste.provider_notify_change(i_queue_name);
    select 200, 'OK' into ret_code, ret_note;
    return;
end;
$$ language plpgsql strict;

