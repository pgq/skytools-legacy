
create or replace function pgq_ext.is_batch_done(
    a_consumer text, a_batch_id bigint)
returns boolean as $$
declare
    res   boolean;
begin
    select last_batch_id = a_batch_id
      into res from pgq_ext.completed_batch
     where consumer_id = a_consumer;
    if not found then
        return false;
    end if;
    return res;
end;
$$ language plpgsql security definer;

create or replace function pgq_ext.set_batch_done(
    a_consumer text, a_batch_id bigint)
returns boolean as $$
begin
    if pgq_ext.is_batch_done(a_consumer, a_batch_id) then
        return false;
    end if;

    if a_batch_id > 0 then
        update pgq_ext.completed_batch
           set last_batch_id = a_batch_id
         where consumer_id = a_consumer;
        if not found then
            insert into pgq_ext.completed_batch (consumer_id, last_batch_id)
                values (a_consumer, a_batch_id);
        end if;
    end if;

    return true;
end;
$$ language plpgsql security definer;

