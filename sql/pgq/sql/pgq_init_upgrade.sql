\set ECHO none
\set VERBOSITY 'terse'
\i ../../upgrade/final/pgq_core_2.1.13.sql
\i ../../upgrade/final/v3.0_pgq_core.sql
\i pgq.upgrade.sql
\set ECHO all

