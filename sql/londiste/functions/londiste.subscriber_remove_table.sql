
create or replace function londiste.subscriber_remove_table(
    i_queue_name text, i_table text)
returns integer as $$
declare
    link  text;
begin
    delete from londiste.subscriber_table
     where queue_name = i_queue_name
       and table_name = i_table;
    if not found then
        raise exception 'no such table';
    end if;

    -- sync link
    link := londiste.link_dest(i_queue_name);
    if link is not null then
        delete from londiste.provider_table
            where queue_name = link
              and table_name = i_table;
        perform londiste.provider_notify_change(link);
    end if;

    return 0;
end;
$$ language plpgsql security definer;

