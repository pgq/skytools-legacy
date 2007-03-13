
create table denytest ( val integer);
insert into denytest values (1);
create trigger xdeny after insert or update or delete
on denytest for each row execute procedure londiste.deny_trigger();

insert into denytest values (2);
update denytest set val = 2;
delete from denytest;

select londiste.disable_deny_trigger(true);
update denytest set val = 2;
select londiste.disable_deny_trigger(true);
update denytest set val = 2;
select londiste.disable_deny_trigger(false);
update denytest set val = 2;
select londiste.disable_deny_trigger(false);
update denytest set val = 2;

