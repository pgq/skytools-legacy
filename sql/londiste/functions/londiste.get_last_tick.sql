
create or replace function londiste.get_last_tick(i_consumer text)
returns bigint as $$
declare
    res   bigint;
begin
    select last_tick_id into res
      from londiste.completed
     where consumer_id = i_consumer;
    return res;
end;
$$ language plpgsql security definer strict stable;

