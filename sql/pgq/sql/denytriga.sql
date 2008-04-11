
create table denytest (
    id integer
);

create trigger denytrg after insert or update or delete
on denytest for each row execute procedure pgq.denytriga('baz');

insert into denytest values (1); -- must fail
select pgq.set_connection_context('foo');
insert into denytest values (1); -- must fail
select pgq.set_connection_context('baz');
insert into denytest values (1); -- must succeed
select pgq.set_connection_context(null);
delete from denytest; -- must fail
select pgq.set_connection_context('baz');
delete from denytest; -- must succeed

