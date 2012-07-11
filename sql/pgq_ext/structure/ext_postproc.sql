
-- tag data objects as dumpable

SELECT pg_catalog.pg_extension_config_dump('pgq_ext.completed_tick', '');
SELECT pg_catalog.pg_extension_config_dump('pgq_ext.completed_batch', '');
SELECT pg_catalog.pg_extension_config_dump('pgq_ext.completed_event', '');
SELECT pg_catalog.pg_extension_config_dump('pgq_ext.partial_batch', '');

