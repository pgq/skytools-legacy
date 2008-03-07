\set ECHO off
set log_error_verbosity = 'terse';
\i ../txid/txid.sql
\i ../pgq/pgq.sql
\i ../pgq_set/pgq_set.sql
\i londiste.sql
\set ECHO all

