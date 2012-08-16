
create extension pgq;

\set ECHO none
\i structure/install.sql
\set ECHO all

create extension pgq_coop from 'unpackaged';
drop extension pgq_coop;

create extension pgq_coop;

