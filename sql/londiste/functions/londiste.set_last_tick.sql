
create or replace function londiste.set_last_tick(
    i_consumer text,
    i_tick_id bigint)
returns integer as $$
begin
    if i_tick_id is null then
        delete from londiste.completed
         where consumer_id = i_consumer;
    else
        update londiste.completed
           set last_tick_id = i_tick_id
         where consumer_id = i_consumer;
        if not found then
            insert into londiste.completed (consumer_id, last_tick_id)
                values (i_consumer, i_tick_id);
        end if;
    end if;

    return 1;
end;
$$ language plpgsql security definer;

