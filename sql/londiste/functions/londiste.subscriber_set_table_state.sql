
create or replace function londiste.subscriber_set_table_state(
    i_queue_name text,
    i_table_name text,
    i_snapshot text,
    i_merge_state text)
returns integer as $$
declare
    link  text;
    ok    integer;
begin
    update londiste.subscriber_table
        set snapshot = i_snapshot,
            merge_state = i_merge_state,
            -- reset skip_snapshot when table is copied over
            skip_truncate = case when i_merge_state = 'ok'
                                 then null
                                 else skip_truncate
                            end
      where queue_name = i_queue_name
        and table_name = i_table_name;
    if not found then
        raise exception 'no such table';
    end if;

    -- sync link state also
    link := londiste.link_dest(i_queue_name);
    if link then
        select * from londiste.provider_table
            where queue_name = linkdst
              and table_name = i_table_name;
        if found then
            if i_merge_state is null or i_merge_state <> 'ok' then
                delete from londiste.provider_table
                 where queue_name = link
                   and table_name = i_table_name;
                perform londiste.notify_change(link);
            end if;
        else
            if i_merge_state = 'ok' then
                insert into londiste.provider_table (queue_name, table_name)
                    values (link, i_table_name);
                perform londiste.notify_change(link);
            end if;
        end if;
    end if;

    return 1;
end;
$$ language plpgsql security definer;

create or replace function londiste.set_table_state(
    i_queue_name text,
    i_table_name text,
    i_snapshot text,
    i_merge_state text)
returns integer as $$
begin
    return londiste.subscriber_set_table_state(i_queue_name, i_table_name, i_snapshot, i_merge_state);
end;
$$ language plpgsql security definer;


