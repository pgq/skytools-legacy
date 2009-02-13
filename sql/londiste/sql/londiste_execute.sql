
set log_error_verbosity = 'terse';

select * from londiste.execute_start('branch_set', 'DDL-A.sql', 'drop all', false);
select * from londiste.execute_start('branch_set', 'DDL-A.sql', 'drop all', false);

select * from londiste.execute_finish('branch_set', 'DDL-A.sql');
select * from londiste.execute_finish('branch_set', 'DDL-A.sql');
select * from londiste.execute_finish('branch_set', 'DDL-XXX.sql');

select * from londiste.execute_start('branch_set', 'DDL-B.sql', 'drop all', true);
select * from londiste.execute_start('branch_set', 'DDL-B.sql', 'drop all', true);



select * from londiste.execute_start('aset', 'DDL-root.sql', 'drop all', true);
select * from londiste.execute_start('aset', 'DDL-root.sql', 'drop all', true);
select * from londiste.execute_finish('aset', 'DDL-root.sql');
select * from londiste.execute_finish('aset', 'DDL-root.sql');

