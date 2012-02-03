\set ECHO off

set log_error_verbosity = 'terse';
set client_min_messages = 'fatal';
create language plpgsql;
set client_min_messages = 'warning';

-- \i ../txid/txid.sql
\i ../pgq/pgq.sql
\i ../pgq_node/pgq_node.sql

\i londiste.sql

\set ECHO all

