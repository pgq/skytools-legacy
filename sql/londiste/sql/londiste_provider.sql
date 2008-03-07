
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

select * from pgq_set.add_member('aset', 'rnode', 'dbname=db', false);
select * from pgq_set.create_node('aset', 'root', 'rnode', 'londiste_root', null::text, null::int8, null::text);

select * from londiste.node_add_table('aset', 'public.testdata_nopk');
select * from londiste.node_add_table('aset', 'public.testdata');

select * from londiste.node_add_table('pqueue', 'public.testdata');
select * from londiste.node_add_table('pqueue', 'public.testdata');
select * from londiste.node_add_table('pset', 'public.testdata_nopk');

select londiste.node_refresh_trigger('pqueue', 'public.testdata');

select * from londiste.node_get_table_list('pqueue');

select londiste.node_remove_table('pqueue', 'public.nonexist');
select londiste.node_remove_table('pqueue', 'public.testdata');

select * from londiste.node_get_table_list('pqueue');

--
-- seqs
--

select * from londiste.node_get_seq_list('pqueue');
select londiste.node_add_seq('pqueue', 'public.no_seq');
select londiste.node_add_seq('pqueue', 'public.testdata_id_seq');
select londiste.node_add_seq('pqueue', 'public.testdata_id_seq');
select * from londiste.node_get_seq_list('pqueue');
select londiste.node_remove_seq('pqueue', 'public.testdata_id_seq');
select londiste.node_remove_seq('pqueue', 'public.testdata_id_seq');
select * from londiste.node_get_seq_list('pqueue');

--
-- linked queue
--
select londiste.node_add_table('pqueue', 'public.testdata');
insert into londiste.link (source, dest) values ('mqueue', 'pqueue');


select londiste.node_add_table('pqueue', 'public.testdata');
select londiste.node_remove_table('pqueue', 'public.testdata');

select londiste.node_add_seq('pqueue', 'public.testdata_id_seq');
select londiste.node_remove_seq('pqueue', 'public.testdata_seq');

--
-- cleanup
--

delete from londiste.link;
drop table testdata;
drop table testdata_nopk;
delete from londiste.node_seq;
delete from londiste.node_table;
select pgq.drop_queue('pqueue');

