
set client_min_messages = 'warning';
\set VERBOSITY 'terse'

select 1
from (select set_config(name, 'escape', false) as ignore
                 from pg_settings where name = 'bytea_output') x
where x.ignore = 'foo';


--
-- tables
--
create table leafdata (
    id serial primary key,
    data text
);

select current_database();

select * from pgq_node.register_location('leafq', 'lq_node1', 'dbname=db', false);
select * from pgq_node.register_location('leafq', 'lq_node2', 'dbname=db2', false);
select * from pgq_node.create_node('leafq', 'leaf', 'lq_node2', 'londiste_leaf', 'lq_node1', 100, null::text);

select * from londiste.local_show_missing('leafq');

select * from londiste.local_add_table('leafq', 'public.leafdata');
select * from londiste.global_add_table('leafq', 'public.leafdata');
select * from londiste.local_add_table('leafq', 'public.leafdata');
select * from londiste.global_add_table('leafq', 'public.tmp');
select * from londiste.get_table_list('leafq');

select tgname, tgargs from pg_trigger
where tgrelid = 'public.leafdata'::regclass
order by 1;

insert into leafdata values (1, 'asd');

select * from londiste.global_remove_table('leafq', 'public.tmp');
select * from londiste.local_remove_table('leafq', 'public.leafdata');
select * from londiste.local_remove_table('leafq', 'public.leafdata');
select * from londiste.get_table_list('leafq');

select * from londiste.local_show_missing('leafq');

