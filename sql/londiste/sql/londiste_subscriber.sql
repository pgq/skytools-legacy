
set client_min_messages = 'warning';

create table testdata (
    id serial primary key,
    data text
);

--
-- tables
--

select londiste.subscriber_add_table('pqueue', 'public.testdata_nopk');
select londiste.subscriber_add_table('pqueue', 'public.testdata');

select pgq.create_queue('pqueue');
select londiste.subscriber_add_table('pqueue', 'public.testdata');
select londiste.subscriber_add_table('pqueue', 'public.testdata');

select * from londiste.subscriber_get_table_list('pqueue');

select londiste.subscriber_remove_table('pqueue', 'public.nonexist');
select londiste.subscriber_remove_table('pqueue', 'public.testdata');

select * from londiste.subscriber_get_table_list('pqueue');

--
-- seqs
--

select * from londiste.subscriber_get_seq_list('pqueue');
select londiste.subscriber_add_seq('pqueue', 'public.no_seq');
select londiste.subscriber_add_seq('pqueue', 'public.testdata_id_seq');
select londiste.subscriber_add_seq('pqueue', 'public.testdata_id_seq');
select * from londiste.subscriber_get_seq_list('pqueue');
select londiste.subscriber_remove_seq('pqueue', 'public.testdata_id_seq');
select londiste.subscriber_remove_seq('pqueue', 'public.testdata_id_seq');
select * from londiste.subscriber_get_seq_list('pqueue');

--
-- linked queue
--
select londiste.subscriber_add_table('pqueue', 'public.testdata');
insert into londiste.link (source, dest) values ('mqueue', 'pqueue');


select londiste.subscriber_add_table('pqueue', 'public.testdata');
select londiste.subscriber_remove_table('pqueue', 'public.testdata');

select londiste.subscriber_add_seq('pqueue', 'public.testdata_id_seq');
select londiste.subscriber_remove_seq('pqueue', 'public.testdata_seq');


