
-- tag data objects as dumpable

SELECT pg_catalog.pg_extension_config_dump('pgq.queue', '');
SELECT pg_catalog.pg_extension_config_dump('pgq.consumer', '');
SELECT pg_catalog.pg_extension_config_dump('pgq.tick', '');
SELECT pg_catalog.pg_extension_config_dump('pgq.subscription', '');
SELECT pg_catalog.pg_extension_config_dump('pgq.event_template', '');
SELECT pg_catalog.pg_extension_config_dump('pgq.retry_queue', '');

---- pg_dump is broken and cannot handle dumpable sequences
-- SELECT pg_catalog.pg_extension_config_dump('pgq.batch_id_seq', '');

