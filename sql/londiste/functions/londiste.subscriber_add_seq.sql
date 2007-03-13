
create or replace function londiste.subscriber_add_seq(
    i_queue_name text, i_seq_name text)
returns integer as $$
declare
    link text;
begin
    insert into londiste.subscriber_seq (queue_name, seq_name)
        values (i_queue_name, i_seq_name);

    -- update linked queue if needed
    link := londiste.link_dest(i_queue_name);
    if link is not null then
        insert into londiste.provider_seq
            (queue_name, seq_name)
        values (link, i_seq_name);
        perform londiste.provider_notify_change(link);
    end if;

    return 0;
end;
$$ language plpgsql security definer;

