

\set ECHO none
set log_error_verbosity = 'terse';
set client_min_messages = 'warning';

\i create_partition.sql
\set ECHO all

create table events (
    id int4 primary key,
    txt text not null,
    ctime timestamptz not null default now(),
    someval int4 check (someval > 0)
);
create index ctime_idx on events (ctime);

select create_partition('events', 'events_2011_01', 'id', 'ctime', '2011-01-01', 'month');
select create_partition('events', 'events_2011_01', 'id', 'ctime', '2011-01-01'::timestamptz, 'month');

select create_partition('events', 'events_2011_01', 'id', 'ctime', '2011-01-01'::timestamp, 'month');

select count(*) from pg_indexes where schemaname='public' and tablename = 'events_2011_01';
select count(*) from pg_constraint where conrelid = 'public.events_2011_01'::regclass;

-- \d events_2011_01

