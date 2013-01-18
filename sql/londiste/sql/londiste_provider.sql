
set client_min_messages = 'warning';
\set VERBOSITY 'terse'

--
-- tables
--
create table testdata (
    id serial primary key,
    txt text
);
create table testdata_nopk (
    id serial,
    txt text
);

select current_database();

select * from pgq_node.register_location('aset', 'rnode', 'dbname=db', false);
select * from pgq_node.create_node('aset', 'root', 'rnode', 'londiste_root', null::text, null::int8, null::text);

select * from londiste.local_add_table('aset', 'public.testdata_nopk');
select * from londiste.local_add_table('aset', 'public.testdata');
select tgname from pg_trigger where tgrelid = 'public.testdata'::regclass order by 1;
insert into testdata (txt) values ('test-data');
select * from londiste.get_table_list('aset');
select * from londiste.local_show_missing('aset');
select * from londiste.local_remove_table('aset', 'public.testdata');
select * from londiste.local_remove_table('aset', 'public.testdata');
select tgname from pg_trigger where tgrelid = 'public.testdata'::regclass;
select * from londiste.get_table_list('aset');

select ev_id, ev_type, ev_data, ev_extra1 from pgq.event_template;

select * from londiste.local_show_missing('aset');

-- trigtest
create table trg_test (
    id int4 primary key,
    txt text
);

select * from londiste.local_add_table('aset', 'public.trg_test', array['ev_extra4=''test='' || txt']);
select * from londiste.local_add_table('aset', 'public.trg_test');
select * from londiste.local_add_table('aset', 'public.trg_test', array['ev_extra4=''test='' || txt'], 'handler=foobar');
insert into trg_test values (1, 'data');
truncate trg_test;
select ev_id, ev_type, ev_data, ev_extra1, ev_extra4 from pgq.event_template where ev_extra1 = 'public.trg_test';

select tgname from pg_trigger where tgrelid = 'public.trg_test'::regclass order by 1;
delete from londiste.table_info where table_name = 'public.trg_test';
select tgname from pg_trigger where tgrelid = 'public.trg_test'::regclass order by 1;

-- handler test
create table hdlr_test (
    id int4 primary key,
    txt text
);

select * from londiste.local_add_table('aset', 'public.hdlr_test');
insert into hdlr_test values (1, 'data');

select * from londiste.local_change_handler('aset', 'public.hdlr_test', array['ev_extra4=''test='' || txt'], 'handler=foobar');
insert into hdlr_test values (2, 'data2');

select * from londiste.local_change_handler('aset', 'public.hdlr_test', '{}'::text[], '');
insert into hdlr_test values (3, 'data3');
truncate hdlr_test;

select ev_id, ev_type, ev_data, ev_extra1, ev_extra4 from pgq.event_template where ev_extra1 = 'public.hdlr_test';

-- test proper trigger creation with add-table specific args
select * from londiste.local_add_table('aset', 'public.trg_test', array['ev_extra4=''test='' || txt', 'expect_sync', 'skip']);
insert into trg_test values (2, 'data2');
