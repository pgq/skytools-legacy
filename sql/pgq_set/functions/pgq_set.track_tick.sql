
create or replace function pgq_set.get_completed_tick(a_set text, a_consumer text)
returns int8 as $$
declare
    res   int8;
begin
    select tick_id into res
      from pgq_set.completed_tick
     where set_name = a_set and worker_name = a_consumer;
    return res;
end;
$$ language plpgsql security definer;

create or replace function pgq_set.set_completed_tick(a_set text, a_consumer text, a_tick_id bigint)
returns integer as $$
begin
    if a_tick_id is null then
        delete from pgq_set.completed_tick
         where set_name = a_set and worker_name = a_consumer;
    else   
        update pgq_set.completed_tick
           set tick_id = a_tick_id
         where set_name = a_set and worker_name = a_consumer;
        if not found then
            insert into pgq_set.completed_tick (set_name, worker_name, tick_id)
                values (a_set, a_consumer, a_tick_id);
        end if;
    end if;

    return 1;
end;
$$ language plpgsql security definer;

