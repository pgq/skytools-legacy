
set client_min_messages = 'warning';
\set VERBOSITY 'terse'

--
-- tables
--
create table testdata (
    id serial primary key,
    data text
);
create table testdata_nopk (
    id serial,
    data text
);

select current_database();

select * from pgq_node.register_location('aset', 'rnode', 'dbname=db', false);
select * from pgq_node.create_node('aset', 'root', 'rnode', 'londiste_root', null::text, null::int8, null::text);

select * from londiste.local_add_table('aset', 'public.testdata_nopk');
select * from londiste.local_add_table('aset', 'public.testdata');
insert into testdata (data) values ('test-data');
select * from londiste.get_table_list('aset');
select * from londiste.local_show_missing('aset');
select * from londiste.local_remove_table('aset', 'public.testdata');
select * from londiste.local_remove_table('aset', 'public.testdata');
select * from londiste.get_table_list('aset');

select ev_id, ev_type, ev_data, ev_extra1 from pgq.event_template;

select * from londiste.local_show_missing('aset');

