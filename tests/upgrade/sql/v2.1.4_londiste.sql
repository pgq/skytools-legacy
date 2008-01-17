set default_with_oids = 'off';

create schema londiste;
grant usage on schema londiste to public;

create table londiste.provider_table (
    nr                  serial not null,
    queue_name          text not null,
    table_name          text not null,
    trigger_name        text,
    primary key (queue_name, table_name)
);

create table londiste.provider_seq (
    nr                  serial not null,
    queue_name          text not null,
    seq_name            text not null,
    primary key (queue_name, seq_name)
);

create table londiste.completed (
    consumer_id     text not null,
    last_tick_id    bigint not null,

    primary key (consumer_id)
);

create table londiste.link (
    source    text not null,
    dest      text not null,
    primary key (source),
    unique (dest)
);

create table londiste.subscriber_table (
    nr                  serial not null,
    queue_name          text not null,
    table_name          text not null,
    snapshot            text,
    merge_state         text,
    trigger_name        text,

    skip_truncate       bool,

    primary key (queue_name, table_name)
);

create table londiste.subscriber_seq (
    nr                  serial not null,
    queue_name          text not null,
    seq_name            text not null,

    primary key (queue_name, seq_name)
);


create type londiste.ret_provider_table_list as (
    table_name text,
    trigger_name text
);

create type londiste.ret_subscriber_table as (
    table_name text,
    merge_state text,
    snapshot text,
    trigger_name text,
    skip_truncate bool
);


create or replace function londiste.deny_trigger()
returns trigger as $$
    if 'undeny' in GD:
        return 'OK'
    plpy.error('Changes no allowed on this table')
$$ language plpythonu;

create or replace function londiste.disable_deny_trigger(i_allow boolean)
returns boolean as $$
    if args[0]:
        GD['undeny'] = 1
        return True
    else:
        if 'undeny' in GD:
            del GD['undeny']
        return False
$$ language plpythonu;

create or replace function londiste.find_column_types(tbl text)
returns text as $$
declare
    res      text;
    col      record;
    tbl_oid  oid;
begin
    tbl_oid := londiste.find_table_oid(tbl);
    res := '';
    for col in 
        SELECT CASE WHEN k.attname IS NOT NULL THEN 'k' ELSE 'v' END AS type
            FROM pg_attribute a LEFT JOIN (
                SELECT k.attname FROM pg_index i, pg_attribute k
                 WHERE i.indrelid = tbl_oid AND k.attrelid = i.indexrelid
                   AND i.indisprimary AND k.attnum > 0 AND NOT k.attisdropped
                ) k ON (k.attname = a.attname)
            WHERE a.attrelid = tbl_oid AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
    loop
        res := res || col.type;
    end loop;

    return res;
end;
$$ language plpgsql;

create or replace function londiste.find_rel_oid(tbl text, kind text)
returns oid as $$
declare
    res      oid;
    pos      integer;
    schema   text;
    name     text;
begin
    pos := position('.' in tbl);
    if pos > 0 then
        schema := substring(tbl for pos - 1);
        name := substring(tbl from pos + 1);
    else
        schema := 'public';
        name := tbl;
    end if;
    select c.oid into res
      from pg_namespace n, pg_class c
     where c.relnamespace = n.oid
       and c.relkind = kind
       and n.nspname = schema and c.relname = name;
    if not found then
        if kind = 'r' then
            raise exception 'table not found';
        elsif kind = 'S' then
            raise exception 'seq not found';
        else
            raise exception 'weird relkind';
        end if;
    end if;

    return res;
end;
$$ language plpgsql;

create or replace function londiste.find_table_oid(tbl text)
returns oid as $$
begin
    return londiste.find_rel_oid(tbl, 'r');
end;
$$ language plpgsql;

create or replace function londiste.find_seq_oid(tbl text)
returns oid as $$
begin
    return londiste.find_rel_oid(tbl, 'S');
end;
$$ language plpgsql;


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
$$ language plpgsql security definer;


create or replace function londiste.link_source(i_dst_name text)
returns text as $$
declare
    res  text;
begin
    select source into res from londiste.link
     where dest = i_dst_name;
    return res;
end;
$$ language plpgsql security definer;

create or replace function londiste.link_dest(i_source_name text)
returns text as $$
declare
    res  text;
begin
    select dest into res from londiste.link
     where source = i_source_name;
    return res;
end;
$$ language plpgsql security definer;

create or replace function londiste.cmp_list(list1 text, a_queue text, a_table text, a_field text)
returns boolean as $$
declare
    sql   text;
    tmp   record;
    list2 text;
begin
    sql := 'select ' || a_field || ' as name from ' || a_table
        || ' where queue_name = ' || quote_literal(a_queue)
        || ' order by 1';
    list2 := '';
    for tmp in execute sql loop
        if list2 = '' then
            list2 := tmp.name;
        else
            list2 := list2 || ',' || tmp.name;
        end if;
    end loop;
    return list1 = list2;
end;
$$ language plpgsql;

create or replace function londiste.link(i_source_name text, i_dest_name text, prov_tick_id bigint, prov_tbl_list text, prov_seq_list text)
returns text as $$
declare
    tmp  text;
    list text;
    tick_seq text;
    external boolean;
    last_tick bigint;
begin
    -- check if all matches
    if not londiste.cmp_list(prov_tbl_list, i_source_name,
                             'londiste.subscriber_table', 'table_name')
    then
        raise exception 'not all tables copied into subscriber';
    end if;
    if not londiste.cmp_list(prov_seq_list, i_source_name,
                             'londiste.subscriber_seq', 'seq_name')
    then
        raise exception 'not all seqs copied into subscriber';
    end if;
    if not londiste.cmp_list(prov_seq_list, i_dest_name,
                             'londiste.provider_table', 'table_name')
    then
        raise exception 'linked provider queue does not have all tables';
    end if;
    if not londiste.cmp_list(prov_seq_list, i_dest_name,
                             'londiste.provider_seq', 'seq_name')
    then
        raise exception 'linked provider queue does not have all seqs';
    end if;

    -- check pgq
    select queue_external_ticker, queue_tick_seq into external, tick_seq
        from pgq.queue where queue_name = i_dest_name;
    if not found then
        raise exception 'dest queue does not exist';
    end if;
    if external then
        raise exception 'dest queue has already external_ticker turned on?';
    end if;

    if nextval(tick_seq) >= prov_tick_id then
        raise exception 'dest queue ticks larger';
    end if;
    
    update pgq.queue set queue_external_ticker = true
        where queue_name = i_dest_name;

    insert into londiste.link (source, dest) values (i_source_name, i_dest_name);

    return null;
end;
$$ language plpgsql security definer;

create or replace function londiste.link_del(i_source_name text, i_dest_name text)
returns text as $$
begin
    delete from londiste.link
     where source = i_source_name
       and dest = i_dest_name;
    if not found then
        raise exception 'no suck link';
    end if;
    return null;
end;
$$ language plpgsql security definer;


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
    perform londiste.provider_notify_change(i_queue_name);

    return 0;
end;
$$ language plpgsql security definer;

create or replace function londiste.provider_add_table(
    i_queue_name    text,
    i_table_name    text,
    i_col_types     text
) returns integer strict as $$
declare
    tgname text;
    sql    text;
begin
    if londiste.link_source(i_queue_name) is not null then
        raise exception 'Linked queue, manipulation not allowed';
    end if;

    if position('k' in i_col_types) < 1 then
        raise exception 'need key column';
    end if;
    if position('.' in i_table_name) < 1 then
        raise exception 'need fully-qualified table name';
    end if;
    select queue_name into tgname
        from pgq.queue where queue_name = i_queue_name;
    if not found then
        raise exception 'no such event queue';
    end if;

    tgname := i_queue_name || '_logger';
    tgname := replace(lower(tgname), '.', '_');
    insert into londiste.provider_table
        (queue_name, table_name, trigger_name)
        values (i_queue_name, i_table_name, tgname);

    perform londiste.provider_create_trigger(
        i_queue_name, i_table_name, i_col_types);

    return 1;
end;
$$ language plpgsql security definer;

create or replace function londiste.provider_add_table(
    i_queue_name text,
    i_table_name text
) returns integer as $$
begin
    return londiste.provider_add_table(i_queue_name, i_table_name,
        londiste.find_column_types(i_table_name));
end;
$$ language plpgsql;


create or replace function londiste.provider_create_trigger(
    i_queue_name    text,
    i_table_name    text,
    i_col_types     text
) returns integer strict as $$
declare
    tgname text;
    sql    text;
begin
    select trigger_name into tgname
        from londiste.provider_table
        where queue_name = i_queue_name
          and table_name = i_table_name;
    if not found then
        raise exception 'table not found';
    end if;

    sql := 'select pgq.insert_event('
        || quote_literal(i_queue_name)
        || ', $1, $2, '
        || quote_literal(i_table_name)
        || ', NULL, NULL, NULL)';
    execute 'create trigger ' || tgname
        || ' after insert or update or delete on '
        || i_table_name
        || ' for each row execute procedure logtriga($arg1$'
        || i_col_types || '$arg1$, $arg2$' || sql || '$arg2$)';

    return 1;
end;
$$ language plpgsql security definer;


create or replace function londiste.provider_get_seq_list(i_queue_name text)
returns setof text as $$
declare
    rec record;
begin
    for rec in
        select seq_name from londiste.provider_seq
            where queue_name = i_queue_name
            order by nr
    loop
        return next rec.seq_name;
    end loop;
    return;
end;
$$ language plpgsql security definer;


create or replace function londiste.provider_get_table_list(i_queue text)
returns setof londiste.ret_provider_table_list as $$ 
declare 
    rec   londiste.ret_provider_table_list%rowtype; 
begin 
    for rec in 
        select table_name, trigger_name
            from londiste.provider_table
            where queue_name = i_queue
            order by nr
    loop
        return next rec;
    end loop; 
    return;
end; 
$$ language plpgsql security definer;


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
$$ language plpgsql;


create or replace function londiste.provider_refresh_trigger(
    i_queue_name    text,
    i_table_name    text,
    i_col_types     text
) returns integer strict as $$
declare
    t_name   text;
    tbl_oid  oid;
begin
    select trigger_name into t_name
        from londiste.provider_table
        where queue_name = i_queue_name
          and table_name = i_table_name;
    if not found then
        raise exception 'table not found';
    end if;

    tbl_oid := londiste.find_table_oid(i_table_name);
    perform 1 from pg_trigger
        where tgrelid = tbl_oid
          and tgname = t_name;
    if found then
        execute 'drop trigger ' || t_name || ' on ' || i_table_name;
    end if;

    perform londiste.provider_create_trigger(i_queue_name, i_table_name, i_col_types);

    return 1;
end;
$$ language plpgsql security definer;

create or replace function londiste.provider_refresh_trigger(
    i_queue_name    text,
    i_table_name    text
) returns integer strict as $$
begin
    return londiste.provider_refresh_trigger(i_queue_name, i_table_name,
                            londiste.find_column_types(i_table_name));
end;
$$ language plpgsql security definer;




create or replace function londiste.provider_remove_seq(
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

    delete from londiste.provider_seq
        where queue_name = i_queue_name
          and seq_name = i_seq_name;
    if not found then
        raise exception 'seq not attached';
    end if;

    perform londiste.provider_notify_change(i_queue_name);

    return 0;
end;
$$ language plpgsql security definer;


create or replace function londiste.provider_remove_table(
    i_queue_name   text,
    i_table_name   text
) returns integer as $$
declare
    tgname text;
begin
    if londiste.link_source(i_queue_name) is not null then
        raise exception 'Linked queue, manipulation not allowed';
    end if;

    select trigger_name into tgname from londiste.provider_table
        where queue_name = i_queue_name
          and table_name = i_table_name;
    if not found then
        raise exception 'no such table registered';
    end if;

    execute 'drop trigger ' || tgname || ' on ' || i_table_name;

    delete from londiste.provider_table
        where queue_name = i_queue_name
          and table_name = i_table_name;

    return 1;
end;
$$ language plpgsql security definer;



create or replace function londiste.set_last_tick(
    i_consumer text,
    i_tick_id bigint)
returns integer as $$
begin
    update londiste.completed
       set last_tick_id = i_tick_id
     where consumer_id = i_consumer;
    if not found then
        insert into londiste.completed (consumer_id, last_tick_id)
            values (i_consumer, i_tick_id);
    end if;

    return 1;
end;
$$ language plpgsql security definer;


create or replace function londiste.subscriber_add_seq(
    i_queue_name text, i_seq_name text)
returns integer as $$
declare
    link text;
begin
    insert into londiste.subscriber_seq (queue_name, seq_name)
        values (i_queue_name, i_seq_name);

    -- update linked queue if needed
    link := londiste.link_dest(i_queue_name);
    if link is not null then
        insert into londiste.provider_seq
            (queue_name, seq_name)
        values (link, i_seq_name);
        perform londiste.provider_notify_change(link);
    end if;

    return 0;
end;
$$ language plpgsql security definer;


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


create or replace function londiste.subscriber_get_seq_list(i_queue_name text)
returns setof text as $$
declare
    rec record;
begin
    for rec in
        select seq_name from londiste.subscriber_seq
            where queue_name = i_queue_name
            order by nr
    loop
        return next rec.seq_name;
    end loop;
    return;
end;
$$ language plpgsql security definer;


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


create or replace function londiste.subscriber_remove_seq(
    i_queue_name text, i_seq_name text)
returns integer as $$
declare
    link text;
begin
    delete from londiste.subscriber_seq
        where queue_name = i_queue_name
          and seq_name = i_seq_name;
    if not found then
        raise exception 'no such seq?';
    end if;

    -- update linked queue if needed
    link := londiste.link_dest(i_queue_name);
    if link is not null then
        delete from londiste.provider_seq
         where queue_name = link
           and seq_name = i_seq_name;
        perform londiste.provider_notify_change(link);
    end if;

    return 0;
end;
$$ language plpgsql security definer;


create or replace function londiste.subscriber_remove_table(
    i_queue_name text, i_table text)
returns integer as $$
declare
    link  text;
begin
    delete from londiste.subscriber_table
     where queue_name = i_queue_name
       and table_name = i_table;
    if not found then
        raise exception 'no such table';
    end if;

    -- sync link
    link := londiste.link_dest(i_queue_name);
    if link is not null then
        delete from londiste.provider_table
            where queue_name = link
              and table_name = i_table;
        perform londiste.provider_notify_change(link);
    end if;

    return 0;
end;
$$ language plpgsql;


create or replace function londiste.subscriber_set_skip_truncate(
    i_queue text,
    i_table text,
    i_value bool)
returns integer as $$
begin
    update londiste.subscriber_table
       set skip_truncate = i_value
     where queue_name = i_queue
       and table_name = i_table;
    if not found then
        raise exception 'table not found';
    end if;

    return 1;
end;
$$ language plpgsql security definer;


create or replace function londiste.subscriber_set_table_state(
    i_queue_name text,
    i_table_name text,
    i_snapshot text,
    i_merge_state text)
returns integer as $$
declare
    link  text;
    ok    integer;
begin
    update londiste.subscriber_table
        set snapshot = i_snapshot,
            merge_state = i_merge_state,
            -- reset skip_snapshot when table is copied over
            skip_truncate = case when i_merge_state = 'ok'
                                 then null
                                 else skip_truncate
                            end
      where queue_name = i_queue_name
        and table_name = i_table_name;
    if not found then
        raise exception 'no such table';
    end if;

    -- sync link state also
    link := londiste.link_dest(i_queue_name);
    if link then
        select * from londiste.provider_table
            where queue_name = linkdst
              and table_name = i_table_name;
        if found then
            if i_merge_state is null or i_merge_state <> 'ok' then
                delete from londiste.provider_table
                 where queue_name = link
                   and table_name = i_table_name;
                perform londiste.notify_change(link);
            end if;
        else
            if i_merge_state = 'ok' then
                insert into londiste.provider_table (queue_name, table_name)
                    values (link, i_table_name);
                perform londiste.notify_change(link);
            end if;
        end if;
    end if;

    return 1;
end;
$$ language plpgsql security definer;

create or replace function londiste.set_table_state(
    i_queue_name text,
    i_table_name text,
    i_snapshot text,
    i_merge_state text)
returns integer as $$
begin
    return londiste.subscriber_set_table_state(i_queue_name, i_table_name, i_snapshot, i_merge_state);
end;
$$ language plpgsql security definer;


