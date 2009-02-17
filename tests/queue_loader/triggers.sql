
create trigger logger after insert or update or delete on data.simple_tbl
for each row execute procedure pgq.logutriga('loaderq');

create trigger logger after insert or update or delete on data.bulk_tbl
for each row execute procedure pgq.logutriga('loaderq');

create trigger logger after insert or update or delete on data.keep_all_tbl
for each row execute procedure pgq.logutriga('loaderq');

create trigger logger after insert or update or delete on data.keep_latest_tbl
for each row execute procedure pgq.logutriga('loaderq');

create trigger logger after insert or update or delete on data.random_tbl
for each row execute procedure pgq.logutriga('loaderq');

