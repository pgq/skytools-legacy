
create or replace function pgq_ext.get_last_tick(a_consumer text)
returns int8 as $$
declare
    res   int8;
begin
    select last_tick_id into res
      from pgq_ext.completed_tick
     where consumer_id = a_consumer;
    return res;
end;
$$ language plpgsql security definer;

