
create or replace function londiste.subscriber_get_table_list(i_queue_name text)
returns setof londiste.ret_subscriber_table as $$
declare
    rec londiste.ret_subscriber_table%rowtype;
begin
    for rec in
        select table_name, merge_state, snapshot, trigger_name, skip_truncate
          from londiste.subscriber_table
         where queue_name = i_queue_name
         order by nr
    loop
        return next rec;
    end loop;
    return;
end;
$$ language plpgsql security definer;

-- compat
create or replace function londiste.get_table_state(i_queue text)
returns setof londiste.subscriber_table as $$
declare
    rec londiste.subscriber_table%rowtype;
begin
    for rec in
        select * from londiste.subscriber_table
            where queue_name = i_queue
            order by nr
    loop
        return next rec;
    end loop;
    return;
end;
$$ language plpgsql security definer;

