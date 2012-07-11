
\set ECHO off
\i structure/install.sql
\set ECHO all
create extension pgq_ext from 'unpackaged';
select array_length(extconfig, 1) as dumpable from pg_catalog.pg_extension where extname = 'pgq_ext';
drop extension pgq_ext;

create extension pgq_ext;
select array_length(extconfig, 1) as dumpable from pg_catalog.pg_extension where extname = 'pgq_ext';

