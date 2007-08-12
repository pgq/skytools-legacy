
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

select londiste.provider_add_table('pqueue', 'public.testdata_nopk');
select londiste.provider_add_table('pqueue', 'public.testdata');

select pgq.create_queue('pqueue');
select londiste.provider_add_table('pqueue', 'public.testdata');
select londiste.provider_add_table('pqueue', 'public.testdata');

select londiste.provider_refresh_trigger('pqueue', 'public.testdata');

select * from londiste.provider_get_table_list('pqueue');

select londiste.provider_remove_table('pqueue', 'public.nonexist');
select londiste.provider_remove_table('pqueue', 'public.testdata');

select * from londiste.provider_get_table_list('pqueue');

--
-- seqs
--

select * from londiste.provider_get_seq_list('pqueue');
select londiste.provider_add_seq('pqueue', 'public.no_seq');
select londiste.provider_add_seq('pqueue', 'public.testdata_id_seq');
select londiste.provider_add_seq('pqueue', 'public.testdata_id_seq');
select * from londiste.provider_get_seq_list('pqueue');
select londiste.provider_remove_seq('pqueue', 'public.testdata_id_seq');
select londiste.provider_remove_seq('pqueue', 'public.testdata_id_seq');
select * from londiste.provider_get_seq_list('pqueue');

--
-- linked queue
--
select londiste.provider_add_table('pqueue', 'public.testdata');
insert into londiste.link (source, dest) values ('mqueue', 'pqueue');


select londiste.provider_add_table('pqueue', 'public.testdata');
select londiste.provider_remove_table('pqueue', 'public.testdata');

select londiste.provider_add_seq('pqueue', 'public.testdata_id_seq');
select londiste.provider_remove_seq('pqueue', 'public.testdata_seq');

--
-- cleanup
--

delete from londiste.link;
drop table testdata;
drop table testdata_nopk;
delete from londiste.provider_seq;
delete from londiste.provider_table;
select pgq.drop_queue('pqueue');

