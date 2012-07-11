\set ECHO off

set log_error_verbosity = 'terse';
set client_min_messages = 'fatal';
create language plpgsql;
set client_min_messages = 'warning';

create extension pgq;
create extension pgq_node;

\i londiste.sql

\set ECHO all

create extension londiste from 'unpackaged';
select array_length(extconfig, 1) as dumpable from pg_catalog.pg_extension where extname = 'londiste';

drop extension londiste;

create extension londiste;
select array_length(extconfig, 1) as dumpable from pg_catalog.pg_extension where extname = 'londiste';

