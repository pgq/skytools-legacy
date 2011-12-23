
set client_min_messages = 'warning';
\set VERBOSITY 'terse'

--
-- tables
--
create table slavedata (
    id serial primary key,
    data text
);

select current_database();

select * from pgq_node.register_location('branch_set', 'snode', 'dbname=db', false);
select * from pgq_node.register_location('branch_set', 'pnode', 'dbname=db2', false);
select * from pgq_node.create_node('branch_set', 'branch', 'snode', 'londiste_branch', 'pnode', 100, null::text);

select * from londiste.local_show_missing('branch_set');

select * from londiste.local_add_table('branch_set', 'public.slavedata');
select * from londiste.global_add_table('branch_set', 'public.slavedata');
select * from londiste.local_add_table('branch_set', 'public.slavedata');
select * from londiste.global_add_table('branch_set', 'public.tmp');
select * from londiste.get_table_list('branch_set');

select * from londiste.local_set_table_state('branch_set', 'public.slavedata', null, 'in-copy');
select * from londiste.get_table_list('branch_set');

select * from londiste.global_remove_table('branch_set', 'public.tmp');
select * from londiste.local_remove_table('branch_set', 'public.slavedata');
select * from londiste.local_remove_table('branch_set', 'public.slavedata');
select * from londiste.get_table_list('branch_set');

select * from londiste.local_show_missing('branch_set');

