
create or replace function londiste.provider_remove_seq(
    i_queue_name text, i_seq_name text)
returns integer as $$
declare
    link text;
begin
    -- check if linked queue
    link := londiste.link_source(i_queue_name);
    if link is not null then
        raise exception 'Linked queue, cannot modify';
    end if;

    delete from londiste.provider_seq
        where queue_name = i_queue_name
          and seq_name = i_seq_name;
    if not found then
        raise exception 'seq not attached';
    end if;

    perform londiste.provider_notify_change(i_queue_name);

    return 0;
end;
$$ language plpgsql security definer;

