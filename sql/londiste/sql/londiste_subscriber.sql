
set client_min_messages = 'warning';
\set VERBOSITY 'terse'

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

--
-- skip-truncate, set_table_state
--

select londiste.subscriber_add_table('pqueue', 'public.skiptest');
select skip_truncate from londiste.subscriber_table where table_name = 'public.skiptest';
select londiste.subscriber_set_skip_truncate('pqueue', 'public.skiptest', true);
select skip_truncate from londiste.subscriber_table where table_name = 'public.skiptest';
select londiste.subscriber_set_table_state('pqueue', 'public.skiptest', 'snap1', 'in-copy');
select skip_truncate, snapshot from londiste.subscriber_table where table_name = 'public.skiptest';
select londiste.subscriber_set_table_state('pqueue', 'public.skiptest', null, 'ok');
select skip_truncate, snapshot from londiste.subscriber_table where table_name = 'public.skiptest';

--
-- test tick tracking
--
select londiste.get_last_tick('c');
select londiste.set_last_tick('c', 1);
select londiste.get_last_tick('c');
select londiste.set_last_tick('c', 2);
select londiste.get_last_tick('c');
select londiste.set_last_tick('c', NULL);
select londiste.get_last_tick('c');

