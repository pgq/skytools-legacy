
set client_min_messages = 'warning';
\set VERBOSITY 'terse'

--
-- tables
--
create table tblmerge (
    id int4 primary key,
    data text
);

select * from pgq_node.register_location('combined_set', 'croot', 'dbname=db', false);
select * from pgq_node.create_node('combined_set', 'root', 'croot', 'londiste_croot', null, null, null);

select * from pgq_node.register_location('part1_set', 'p1root', 'dbname=db', false);
select * from pgq_node.register_location('part1_set', 'p1merge', 'dbname=db2', false);
select * from pgq_node.create_node('part1_set', 'leaf', 'p1merge', 'londiste_p1merge', 'p1root', 100, 'combined_set');

select * from pgq_node.register_location('part2_set', 'p2root', 'dbname=db', false);
select * from pgq_node.register_location('part2_set', 'p2merge', 'dbname=db2', false);
select * from pgq_node.create_node('part2_set', 'leaf', 'p2merge', 'londiste_p2merge', 'p2root', 100, 'combined_set');

select * from pgq_node.register_location('part3_set', 'p3root', 'dbname=db', false);
select * from pgq_node.register_location('part3_set', 'p3merge', 'dbname=db3', false);
select * from pgq_node.create_node('part3_set', 'leaf', 'p3merge', 'londiste_p3merge', 'p3root', 100, 'combined_set');



select * from londiste.local_add_table('combined_set', 'tblmerge');
select * from londiste.global_add_table('part1_set', 'tblmerge');
select * from londiste.global_add_table('part2_set', 'tblmerge');
select * from londiste.local_add_table('part1_set', 'tblmerge', array['merge_all']);

select * from londiste.get_table_list('part1_set');
select * from londiste.get_table_list('part2_set');
select * from londiste.get_table_list('combined_set');

select * from londiste.local_set_table_state('part1_set', 'public.tblmerge', null, 'in-copy');
select * from londiste.local_set_table_state('part2_set', 'public.tblmerge', null, 'in-copy');
select * from londiste.get_table_list('part1_set');
select * from londiste.get_table_list('part2_set');

select * from londiste.local_set_table_struct('part1_set', 'public.tblmerge', 'create index;');
select * from londiste.get_table_list('part1_set');
select * from londiste.get_table_list('part2_set');

select * from londiste.local_set_table_state('part2_set', 'public.tblmerge', null, 'catching-up');
select * from londiste.get_table_list('part1_set');
select * from londiste.get_table_list('part2_set');

select * from londiste.local_set_table_state('part1_set', 'public.tblmerge', null, 'catching-up');
select * from londiste.get_table_list('part1_set');
select * from londiste.get_table_list('part2_set');

select * from londiste.local_set_table_struct('part1_set', 'public.tblmerge', null);
select * from londiste.get_table_list('part1_set');
select * from londiste.get_table_list('part2_set');

-- test automatic registration on combined-root
select * from londiste.global_add_table('part1_set', 'tblauto');
select * from londiste.global_add_table('part2_set', 'tblauto');
select * from londiste.local_add_table('part1_set', 'tblauto', array['merge_all', 'virtual_table'], 'handler=vtable');
select * from londiste.get_table_list('part2_set');
select * from londiste.get_table_list('combined_set');

--
-- Test all combinations on 3-node merge
--

select * from londiste.global_add_table('part3_set', 'tblmerge');

\set ECHO off

create table states ( state text );
insert into states values ('in-copy');
insert into states values ('!in-copy');
insert into states values ('catching-up');
insert into states values ('!catching-up');

create or replace function testmerge(
    in p1state text, in p2state text, in p3state text,
    out p1res text, out p2res text, out p3res text)
as $$
declare
    p1ddl text;
    p2ddl text;
    p3ddl text;
    tbl text = 'public.tblmerge';
begin
    if position('!' in p1state) > 0 then
        p1ddl := 'x';
    end if;
    if position('!' in p2state) > 0 then
        p2ddl := 'x';
    end if;
    if position('!' in p3state) > 0 then
        p3ddl := 'x';
    end if;

    update londiste.table_info
       set merge_state = replace(p1state, '!', ''), dropped_ddl = p1ddl, local = true
       where table_name = tbl and queue_name = 'part1_set';
    update londiste.table_info
       set merge_state = replace(p2state, '!', ''), dropped_ddl = p2ddl, local = true
       where table_name = tbl and queue_name = 'part2_set';
    update londiste.table_info
       set merge_state = replace(p3state, '!', ''), dropped_ddl = p3ddl, local = true
       where table_name = tbl and queue_name = 'part3_set';

    select coalesce(copy_role, 'NULL') from londiste.get_table_list('part1_set')
        where table_name = tbl into p1res;
    select coalesce(copy_role, 'NULL') from londiste.get_table_list('part2_set')
        where table_name = tbl into p2res;
    select coalesce(copy_role, 'NULL') from londiste.get_table_list('part3_set')
        where table_name = tbl into p3res;
    return;
end;
$$ language plpgsql;

create function testmatrix(
    out p1s text, out p2s text, out p3s text,
    out p1r text, out p2r text, out p3r text)
returns setof record as $$
begin
    for p1s, p2s, p3s in
        select p1.state::name, p2.state::name, p3.state::name
        from states p1, states p2, states p3
        where position('!' in p1.state) + position('!' in p2.state) + position('!' in p3.state) < 2
        order by 1,2,3
    loop
        select * from testmerge(p1s, p2s, p3s) into p1r, p2r, p3r;
        return next;
    end loop;
    return;
end;
$$ language plpgsql;

\set ECHO all

select * from testmatrix();


-- test dropped ddl restore
create table ddlrestore (
    id int4,
    data1 text,
    data2 text
);

select count(*) from pg_indexes where schemaname='public' and tablename='ddlrestore';

insert into londiste.table_info (queue_name, table_name, local, merge_state, dropped_ddl)
values ('part1_set', 'public.ddlrestore', true, 'in-copy', '
ALTER TABLE ddlrestore ADD CONSTRAINT cli_pkey PRIMARY KEY (id);
CREATE INDEX idx_data1 ON ddlrestore USING btree (data1);
CREATE INDEX idx_data2 ON ddlrestore USING btree (data2);
');

select * from londiste.local_remove_table('part1_set', 'public.ddlrestore');

select count(*) from pg_indexes where schemaname='public' and tablename='ddlrestore';

