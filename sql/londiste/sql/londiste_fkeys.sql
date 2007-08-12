
set log_error_verbosity = 'terse';

create table ref_1 (
    id int4 primary key,
    val text
);

create table ref_2 (
    id int4 primary key,
    ref int4 not null references ref_1,
    val text
);

create table ref_3 (
    id int4 primary key,
    ref2 int4 not null references ref_2,
    val text
);

select * from londiste.subscriber_add_table('refqueue', 'public.ref_1');
select * from londiste.subscriber_add_table('refqueue', 'public.ref_2');
select * from londiste.subscriber_add_table('refqueue', 'public.ref_3');

select * from londiste.find_table_fkeys('public.ref_1');
select * from londiste.find_table_fkeys('public.ref_2');
select * from londiste.find_table_fkeys('public.ref_3');

select * from londiste.subscriber_get_table_pending_fkeys('public.ref_2');

select * from londiste.subscriber_get_queue_valid_pending_fkeys('refqueue');

-- drop fkeys

select * from londiste.subscriber_drop_table_fkey('public.ref_2', 'ref_2_ref_fkey');

select * from londiste.find_table_fkeys('public.ref_1');
select * from londiste.find_table_fkeys('public.ref_2');
select * from londiste.find_table_fkeys('public.ref_3');

select * from londiste.subscriber_drop_table_fkey('public.ref_3', 'ref_3_ref2_fkey');

-- check if dropped

select * from londiste.find_table_fkeys('public.ref_1');
select * from londiste.find_table_fkeys('public.ref_2');
select * from londiste.find_table_fkeys('public.ref_3');

-- look state
select * from londiste.subscriber_get_table_pending_fkeys('public.ref_2');
select * from londiste.subscriber_get_queue_valid_pending_fkeys('refqueue');

-- toggle sync
select * from londiste.subscriber_set_table_state('refqueue', 'public.ref_1', null, 'ok');
select * from londiste.subscriber_get_queue_valid_pending_fkeys('refqueue');
select * from londiste.subscriber_set_table_state('refqueue', 'public.ref_2', null, 'ok');
select * from londiste.subscriber_get_queue_valid_pending_fkeys('refqueue');
select * from londiste.subscriber_set_table_state('refqueue', 'public.ref_3', null, 'ok');
select * from londiste.subscriber_get_queue_valid_pending_fkeys('refqueue');

-- restore
select * from londiste.subscriber_restore_table_fkey('public.ref_2', 'ref_2_ref_fkey');
select * from londiste.subscriber_restore_table_fkey('public.ref_3', 'ref_3_ref2_fkey');

-- look state
select * from londiste.subscriber_get_table_pending_fkeys('public.ref_2');
select * from londiste.subscriber_get_queue_valid_pending_fkeys('refqueue');
select * from londiste.find_table_fkeys('public.ref_1');
select * from londiste.find_table_fkeys('public.ref_2');
select * from londiste.find_table_fkeys('public.ref_3');


