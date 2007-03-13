-- init
\set ECHO none
\i logtriga.sql
\set ECHO all

create table rtest (
	id integer primary key,
	dat text
);

create table clog (
  id serial,
  op text,
  data text
);

create trigger rtest_triga after insert or update or delete on rtest
for each row execute procedure logtriga('kv',
'insert into clog (op, data) values ($1, $2)');

-- simple test
insert into rtest values (1, 'value1');
update rtest set dat = 'value2';
delete from rtest;
select * from clog; delete from clog;

-- test new fields
alter table rtest add column dat2 text;
insert into rtest values (1, 'value1');
update rtest set dat = 'value2';
delete from rtest;
select * from clog; delete from clog;

-- test field rename
alter table rtest alter column dat type integer using 0;
insert into rtest values (1, '666', 'newdat');
update rtest set dat = 5;
delete from rtest;
select * from clog; delete from clog;


-- test field ignore
drop trigger rtest_triga on rtest;
create trigger rtest_triga after insert or update or delete on rtest
for each row execute procedure logtriga('kiv',
'insert into clog (op, data) values ($1, $2)');

insert into rtest values (1, '666', 'newdat');
update rtest set dat = 5, dat2 = 'newdat2';
update rtest set dat = 6;
delete from rtest;
select * from clog; delete from clog;


-- test wrong key
drop trigger rtest_triga on rtest;
create trigger rtest_triga after insert or update or delete on rtest
for each row execute procedure logtriga('vik',
'insert into clog (op, data) values ($1, $2)');

insert into rtest values (1, 0, 'non-null');
insert into rtest values (2, 0, NULL);
update rtest set dat2 = 'non-null2' where id=1;
update rtest set dat2 = NULL where id=1;
update rtest set dat2 = 'new-nonnull' where id=2;

delete from rtest where id=1;
delete from rtest where id=2;

select * from clog; delete from clog;




