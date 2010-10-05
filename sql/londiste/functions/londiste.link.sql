
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
    sql := 'select ' || quote_ident(a_field) || ' as name from ' || londiste.quote_fqname(a_table)
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
$$ language plpgsql security definer;

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

