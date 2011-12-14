
create or replace function pgq_ext.set_last_tick(a_consumer text, a_tick_id bigint)
returns integer as $$
begin
    if a_tick_id is null then
        delete from pgq_ext.completed_tick
         where consumer_id = a_consumer;
    else   
        update pgq_ext.completed_tick
           set last_tick_id = a_tick_id
         where consumer_id = a_consumer;
        if not found then
            insert into pgq_ext.completed_tick (consumer_id, last_tick_id)
                values (a_consumer, a_tick_id);
        end if;
    end if;

    return 1;
end;
$$ language plpgsql security definer;

