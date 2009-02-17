
insert into data.simple_tbl (username, contactname, data)
values ('randuser'||random()::text, 'randcontact'||random()::text, 'link');

/*
insert into data.simple_tbl (username, contactname, data)
values ('sameuser', 'samecontact', 'link');
update data.simple_tbl 
*/

insert into data.bulk_tbl (data) values ('newdata');


insert into data.keep_all_tbl (username, data) values ('sameuser', 'newdata');

insert into data.keep_latest_tbl (username, data) values ('sameuser', 'newdata');

insert into data.random_tbl (username, data) values ('sameuser', 'newdata');



