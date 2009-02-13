
create or replace function londiste.root_notify_change(i_queue_name text, i_ev_type text, i_ev_data text)
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

    if not coalesce(pgq_node.is_root_node(i_queue_name), false) then
        raise exception 'only root node can send events';
    end if;
    perform pgq.insert_event(i_queue_name, i_ev_type, i_ev_data);

    return 1;
end;
$$ language plpgsql;

