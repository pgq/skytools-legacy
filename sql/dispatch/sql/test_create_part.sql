

\set ECHO none
set log_error_verbosity = 'terse';
set client_min_messages = 'warning';

\i create_partition.sql
\set ECHO all

drop role if exists ptest1;
drop role if exists ptest2;
create group ptest1;
create group ptest2;

create table events (
    id int4 primary key,
    txt text not null,
    ctime timestamptz not null default now(),
    someval int4 check (someval > 0)
);
create index ctime_idx on events (ctime);

create rule ignore_dups AS
    on insert to events
    where (exists (select 1 from events
                   where (events.id = new.id)))
    do instead nothing;



grant select,delete on events to ptest1;
grant select,update,delete on events to ptest2 with grant option;

select create_partition('events', 'events_2011_01', 'id', 'ctime', '2011-01-01', 'month');
select create_partition('events', 'events_2011_01', 'id', 'ctime', '2011-01-01'::timestamptz, 'month');

select create_partition('events', 'events_2011_01', 'id', 'ctime', '2011-01-01'::timestamp, 'month');

select count(*) from pg_indexes where schemaname='public' and tablename = 'events_2011_01';
select count(*) from pg_constraint where conrelid = 'public.events_2011_01'::regclass;
select count(*) from pg_rules where schemaname = 'public' and tablename = 'events_2011_01';

-- \d events_2011_01
-- \dp events
-- \dp events_2011_01

