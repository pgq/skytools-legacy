
create table qtable (
    data text
);

create trigger qtrigger before insert on qtable
for each row execute procedure pgq.logutriga('testqueue');

create table logtable (
    event_id bigint,
    script text,
    data text
);

select pgq.create_queue('testqueue');
