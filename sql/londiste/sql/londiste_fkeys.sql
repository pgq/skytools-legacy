
set log_error_verbosity = 'terse';
set client_min_messages = 'warning';

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

select * from londiste.global_add_table('branch_set', 'public.ref_1');
select * from londiste.global_add_table('branch_set', 'public.ref_2');
select * from londiste.global_add_table('branch_set', 'public.ref_3');

select * from londiste.local_add_table('branch_set', 'public.ref_1');
select * from londiste.local_add_table('branch_set', 'public.ref_2');
select * from londiste.local_add_table('branch_set', 'public.ref_3');

select * from londiste.find_table_fkeys('public.ref_1');
select * from londiste.find_table_fkeys('public.ref_2');
select * from londiste.find_table_fkeys('public.ref_3');

select * from londiste.get_table_pending_fkeys('public.ref_2');

select * from londiste.get_valid_pending_fkeys('branch_set');

-- drop fkeys

select * from londiste.drop_table_fkey('public.ref_2', 'ref_2_ref_fkey');

select * from londiste.find_table_fkeys('public.ref_1');
select * from londiste.find_table_fkeys('public.ref_2');
select * from londiste.find_table_fkeys('public.ref_3');

select * from londiste.drop_table_fkey('public.ref_3', 'ref_3_ref2_fkey');

-- check if dropped

select * from londiste.find_table_fkeys('public.ref_1');
select * from londiste.find_table_fkeys('public.ref_2');
select * from londiste.find_table_fkeys('public.ref_3');

-- look state
select * from londiste.get_table_pending_fkeys('public.ref_2');
select * from londiste.get_valid_pending_fkeys('branch_set');

-- toggle sync
select * from londiste.local_set_table_state('branch_set', 'public.ref_1', null, 'ok');
select * from londiste.get_valid_pending_fkeys('branch_set');
select * from londiste.local_set_table_state('branch_set', 'public.ref_2', null, 'ok');
select * from londiste.get_valid_pending_fkeys('branch_set');
select * from londiste.local_set_table_state('branch_set', 'public.ref_3', null, 'ok');
select * from londiste.get_valid_pending_fkeys('branch_set');

-- restore
select * from londiste.restore_table_fkey('public.ref_2', 'ref_2_ref_fkey');
select * from londiste.restore_table_fkey('public.ref_3', 'ref_3_ref2_fkey');

-- look state
select * from londiste.get_table_pending_fkeys('public.ref_2');
select * from londiste.get_valid_pending_fkeys('branch_set');
select * from londiste.find_table_fkeys('public.ref_1');
select * from londiste.find_table_fkeys('public.ref_2');
select * from londiste.find_table_fkeys('public.ref_3');


