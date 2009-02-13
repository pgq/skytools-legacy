
set client_min_messages = 'warning';
\set VERBOSITY 'terse'

--
-- tables
--
create table tblmerge (
    id int4 primary key,
    data text
);

select * from pgq_node.register_location('combined_set', 'croot', 'dbname=db', false);
select * from pgq_node.create_node('combined_set', 'root', 'croot', 'londiste_croot', null, null, null);

select * from pgq_node.register_location('part1_set', 'p1root', 'dbname=db', false);
select * from pgq_node.register_location('part1_set', 'p1merge', 'dbname=db2', false);
select * from pgq_node.create_node('part1_set', 'leaf', 'p1merge', 'londiste_p1merge', 'p1root', 100, 'combined_set');

select * from pgq_node.register_location('part2_set', 'p2root', 'dbname=db', false);
select * from pgq_node.register_location('part2_set', 'p2merge', 'dbname=db2', false);
select * from pgq_node.create_node('part2_set', 'leaf', 'p2merge', 'londiste_p2merge', 'p2root', 100, 'combined_set');



select * from londiste.local_add_table('combined_set', 'tblmerge');

select * from londiste.global_add_table('part1_set', 'tblmerge');
select * from londiste.local_add_table('part1_set', 'tblmerge');

select * from londiste.global_add_table('part2_set', 'tblmerge');
select * from londiste.local_add_table('part2_set', 'tblmerge');

select * from londiste.get_table_list('part1_set');
select * from londiste.get_table_list('part2_set');
select * from londiste.get_table_list('combined_set');

select * from londiste.local_set_table_state('part1_set', 'public.tblmerge', null, 'in-copy');
select * from londiste.local_set_table_state('part2_set', 'public.tblmerge', null, 'in-copy');
select * from londiste.get_table_list('part1_set');
select * from londiste.get_table_list('part2_set');

select * from londiste.local_set_table_struct('part1_set', 'public.tblmerge', 'create index;');
select * from londiste.get_table_list('part1_set');
select * from londiste.get_table_list('part2_set');

select * from londiste.local_set_table_state('part1_set', 'public.tblmerge', null, 'in-copy');
select * from londiste.local_set_table_state('part2_set', 'public.tblmerge', null, 'catching-up');
select * from londiste.get_table_list('part1_set');
select * from londiste.get_table_list('part2_set');

select * from londiste.local_set_table_struct('part1_set', 'public.tblmerge', null);
select * from londiste.get_table_list('part1_set');
select * from londiste.get_table_list('part2_set');

select * from londiste.local_set_table_state('part1_set', 'public.tblmerge', null, 'catching-up');
select * from londiste.local_set_table_state('part2_set', 'public.tblmerge', null, 'catching-up');
select * from londiste.get_table_list('part1_set');
select * from londiste.get_table_list('part2_set');











