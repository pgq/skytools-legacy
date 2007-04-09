
create or replace function londiste.subscriber_set_skip_truncate(
    i_queue text,
    i_table text,
    i_value bool)
returns integer as $$
begin
    update londiste.subscriber_table
       set skip_truncate = i_value
     where queue_name = i_queue
       and table_name = i_table;
    if not found then
        raise exception 'table not found';
    end if;

    return 1;
end;
$$ language plpgsql security definer;

