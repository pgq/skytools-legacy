
create extension pgq;

\set ECHO none
\i structure/install.sql
\set ECHO all
create extension pgq_node from unpackaged;
select array_length(extconfig, 1) as dumpable from pg_catalog.pg_extension where extname = 'pgq_node';
drop extension pgq_node;

create extension pgq_node;
select array_length(extconfig, 1) as dumpable from pg_catalog.pg_extension where extname = 'pgq_node';

