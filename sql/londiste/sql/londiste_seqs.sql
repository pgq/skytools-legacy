
set client_min_messages = 'warning';
\set VERBOSITY 'terse'

--
-- sequences
--

create sequence masterseq;
create sequence slaveseq;


select * from pgq_node.register_location('seqroot', 'rnode', 'dbname=db', false);
select * from pgq_node.create_node('seqroot', 'root', 'rnode', 'londiste_root', null::text, null::int8, null::text);

select * from londiste.local_add_seq('seqroot', 'masterseq');
select * from londiste.local_add_seq('seqroot', 'masterseq');
select * from londiste.root_check_seqs('seqroot');
select * from londiste.local_remove_seq('seqroot', 'masterseq');
select * from londiste.local_remove_seq('seqroot', 'masterseq');

select * from londiste.get_seq_list('seqroot');

select ev_id, ev_type, ev_data, ev_extra1 from pgq.event_template where ev_type like '%seq%';

-- subscriber
select * from pgq_node.register_location('seqbranch', 'subnode', 'dbname=db', false);
select * from pgq_node.register_location('seqbranch', 'rootnode', 'dbname=db', false);
select * from pgq_node.create_node('seqbranch', 'branch', 'subnode', 'londiste_branch', 'rootnode', 1, null::text);

select * from londiste.local_add_seq('seqbranch', 'masterseq');
select * from londiste.global_update_seq('seqbranch', 'masterseq', 5);
select * from londiste.local_add_seq('seqbranch', 'masterseq');
select * from londiste.root_check_seqs('seqbranch');
select * from londiste.get_seq_list('seqbranch');
select * from londiste.local_remove_seq('seqbranch', 'masterseq');
select * from londiste.local_remove_seq('seqbranch', 'masterseq');

-- seq auto-removal
create table seqtable (
    id1 serial primary key,
    id2 bigserial not null
);

select * from londiste.local_add_table('seqroot', 'seqtable');
select * from londiste.local_add_seq('seqroot', 'seqtable_id1_seq');
select * from londiste.local_add_seq('seqroot', 'seqtable_id2_seq');

select * from londiste.get_table_list('seqroot');
select * from londiste.get_seq_list('seqroot');

select * from londiste.local_remove_table('seqroot', 'seqtable');

select * from londiste.get_seq_list('seqroot');
