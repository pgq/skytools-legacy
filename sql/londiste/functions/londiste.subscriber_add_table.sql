
create or replace function londiste.subscriber_add_table(
    i_queue_name text, i_table text)
returns integer as $$
begin
    insert into londiste.subscriber_table (queue_name, table_name)
        values (i_queue_name, i_table);

    -- linked queue is updated, when the table is copied

    return 0;
end;
$$ language plpgsql security definer;

