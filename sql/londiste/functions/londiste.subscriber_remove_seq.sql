
create or replace function londiste.subscriber_remove_seq(
    i_queue_name text, i_seq_name text)
returns integer as $$
declare
    link text;
begin
    delete from londiste.subscriber_seq
        where queue_name = i_queue_name
          and seq_name = i_seq_name;
    if not found then
        raise exception 'no such seq?';
    end if;

    -- update linked queue if needed
    link := londiste.link_dest(i_queue_name);
    if link is not null then
        delete from londiste.provider_seq
         where queue_name = link
           and seq_name = i_seq_name;
        perform londiste.provider_notify_change(link);
    end if;

    return 0;
end;
$$ language plpgsql security definer;

