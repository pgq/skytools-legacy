\set ECHO off

set log_error_verbosity = 'terse';
set client_min_messages = 'warning';

drop language if exists plpgsql;
create language plpgsql;

\i ../txid/txid.sql
\i ../pgq/pgq.sql
\i ../pgq_node/pgq_node.sql

\i londiste.sql

\set ECHO all

