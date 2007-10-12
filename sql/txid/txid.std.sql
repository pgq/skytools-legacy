-- ----------
-- txid.sql
--
--	SQL script for loading the transaction ID compatible datatype 
--
--	Copyright (c) 2003-2004, PostgreSQL Global Development Group
--	Author: Jan Wieck, Afilias USA INC.
--
-- ----------

set client_min_messages = 'warning';

CREATE DOMAIN txid AS bigint CHECK (value > 0);

--
-- A special transaction snapshot data type for faster visibility checks
--
CREATE OR REPLACE FUNCTION txid_snapshot_in(cstring)
	RETURNS txid_snapshot
	AS 'MODULE_PATHNAME' LANGUAGE C
	IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION txid_snapshot_out(txid_snapshot)
	RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C
	IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION txid_snapshot_recv(internal)
	RETURNS txid_snapshot
	AS 'MODULE_PATHNAME' LANGUAGE C
	IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION txid_snapshot_send(txid_snapshot)
	RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C
	IMMUTABLE STRICT;

--
-- The data type itself
--
CREATE TYPE txid_snapshot (
	INPUT = txid_snapshot_in,
	OUTPUT = txid_snapshot_out,
    RECEIVE = txid_snapshot_recv,
    SEND = txid_snapshot_send,
	INTERNALLENGTH = variable,
	STORAGE = extended,
	ALIGNMENT = double
);

--CREATE OR REPLACE FUNCTION get_current_txid()
CREATE OR REPLACE FUNCTION txid_current()
	RETURNS bigint
	AS 'MODULE_PATHNAME', 'txid_current' LANGUAGE C
	STABLE SECURITY DEFINER;

-- CREATE OR REPLACE FUNCTION get_current_snapshot()
CREATE OR REPLACE FUNCTION txid_current_snapshot()
	RETURNS txid_snapshot
	AS 'MODULE_PATHNAME', 'txid_current_snapshot' LANGUAGE C
	STABLE SECURITY DEFINER;

--CREATE OR REPLACE FUNCTION get_snapshot_xmin(txid_snapshot)
CREATE OR REPLACE FUNCTION txid_snapshot_xmin(txid_snapshot)
	RETURNS bigint
	AS 'MODULE_PATHNAME', 'txid_snapshot_xmin' LANGUAGE C
	IMMUTABLE STRICT;

-- CREATE OR REPLACE FUNCTION get_snapshot_xmax(txid_snapshot)
CREATE OR REPLACE FUNCTION txid_snapshot_xmax(txid_snapshot)
	RETURNS bigint
	AS 'MODULE_PATHNAME', 'txid_snapshot_xmax' LANGUAGE C
	IMMUTABLE STRICT;

-- CREATE OR REPLACE FUNCTION get_snapshot_active(txid_snapshot)
CREATE OR REPLACE FUNCTION txid_snapshot_xip(txid_snapshot)
	RETURNS setof bigint
	AS 'MODULE_PATHNAME', 'txid_snapshot_xip' LANGUAGE C
	IMMUTABLE STRICT;


--
-- Special comparision functions used by the remote worker
-- for sync chunk selection
--
CREATE OR REPLACE FUNCTION txid_visible_in_snapshot(bigint, txid_snapshot)
	RETURNS boolean
	AS 'MODULE_PATHNAME', 'txid_visible_in_snapshot' LANGUAGE C
	IMMUTABLE STRICT;
/*
CREATE OR REPLACE FUNCTION txid_in_snapshot(bigint, txid_snapshot)
	RETURNS boolean
	AS 'MODULE_PATHNAME', 'txid_in_snapshot' LANGUAGE C
	IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION txid_not_in_snapshot(bigint, txid_snapshot)
	RETURNS boolean
	AS 'MODULE_PATHNAME', 'txid_not_in_snapshot' LANGUAGE C
	IMMUTABLE STRICT;
*/
