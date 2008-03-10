
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

select * from pgq_set.add_member('branch_set', 'snode', 'dbname=db', false);
select * from pgq_set.add_member('branch_set', 'pnode', 'dbname=db2', false);
select * from pgq_set.create_node('branch_set', 'branch', 'snode', 'londiste_branch', 'pnode', 100, null::text);

select * from londiste.node_add_table('branch_set', 'public.slavedata');
select * from londiste.set_add_table('branch_set', 'public.slavedata');
select * from londiste.node_add_table('branch_set', 'public.slavedata');
select * from londiste.node_get_table_list('branch_set');
select * from londiste.node_remove_table('branch_set', 'public.slavedata');
select * from londiste.node_remove_table('branch_set', 'public.slavedata');
select * from londiste.node_get_table_list('branch_set');

