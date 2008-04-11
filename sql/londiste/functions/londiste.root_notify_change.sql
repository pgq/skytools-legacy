
create or replace function londiste.root_notify_change(i_set_name text, i_ev_type text, i_ev_data text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: londiste.root_notify_change(3)
--
--      Send event about change in root downstream.
-- ----------------------------------------------------------------------
declare
    que     text;
    ntype   text;
begin
    select s.queue_name, s.node_type into que, ntype
        from pgq_set.set_info s
        where s.set_name = i_set_name;
    if not found then
        raise exception 'Unknown set: %', i_set_name;
    end if;
    if ntype <> 'root' then
        raise exception 'only root node can send events';
    end if;

    perform pgq.insert_event(que, i_ev_type, i_ev_data);

    return 1;
end;
$$ language plpgsql;

