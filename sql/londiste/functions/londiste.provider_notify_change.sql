
create or replace function londiste.provider_notify_change(i_queue_name text)
returns integer as $$
declare
    res      text;
    tbl      record;
begin
    res := '';
    for tbl in
        select table_name from londiste.provider_table
            where queue_name = i_queue_name
            order by nr
    loop
        if res = '' then
            res := tbl.table_name;
        else
            res := res || ',' || tbl.table_name;
        end if;
    end loop;
    
    perform pgq.insert_event(i_queue_name, 'T', res);

    return 1;
end;
$$ language plpgsql security definer;

