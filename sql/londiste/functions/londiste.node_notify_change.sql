
create or replace function londiste.node_send_event(i_set_name text, i_ev_type text, i_ev_data text)
returns integer as $$
declare
    que     text;
begin
    select s.queue_name into que
        from pgq_set s
        where s.set_name = i_set_name;
    if not found then
        raise exception 'Unknown set: %', i_set_name;
    end if;

    perform pgq.insert_event(que, i_ev_data, i_ev_data);

    return 1;
end;
$$ language plpgsql;

