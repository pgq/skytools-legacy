
create or replace function londiste.provider_add_seq(
    i_queue_name text, i_seq_name text)
returns integer as $$
declare
    link text;
begin
    -- check if linked queue
    link := londiste.link_source(i_queue_name);
    if link is not null then
        raise exception 'Linked queue, cannot modify';
    end if;

    perform 1 from pg_class
        where oid = londiste.find_seq_oid(i_seq_name);
    if not found then
        raise exception 'seq not found';
    end if;

    insert into londiste.provider_seq (queue_name, seq_name)
        values (i_queue_name, i_seq_name);

    return 0;
end;
$$ language plpgsql security definer;

