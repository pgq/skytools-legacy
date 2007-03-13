
select pgq.create_queue('data.src');
select pgq.create_queue('data.middle');

create trigger test_logger after insert or update or delete
on data1 for each row execute procedure pgq.logutriga('data.src');

