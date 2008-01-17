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
	AS '$libdir/txid' LANGUAGE C
	IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION txid_snapshot_out(txid_snapshot)
	RETURNS cstring
	AS '$libdir/txid' LANGUAGE C
	IMMUTABLE STRICT;

--
-- The data type itself
--
CREATE TYPE txid_snapshot (
	INPUT = txid_snapshot_in,
	OUTPUT = txid_snapshot_out,
	INTERNALLENGTH = variable,
	STORAGE = extended,
	ALIGNMENT = double
);

CREATE OR REPLACE FUNCTION get_current_txid()
	RETURNS bigint
	AS '$libdir/txid', 'txid_current' LANGUAGE C
	SECURITY DEFINER;

CREATE OR REPLACE FUNCTION get_current_snapshot()
	RETURNS txid_snapshot
	AS '$libdir/txid', 'txid_current_snapshot' LANGUAGE C
	SECURITY DEFINER;

CREATE OR REPLACE FUNCTION get_snapshot_xmin(txid_snapshot)
	RETURNS bigint
	AS '$libdir/txid', 'txid_snapshot_xmin' LANGUAGE C
	IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_snapshot_xmax(txid_snapshot)
	RETURNS bigint
	AS '$libdir/txid', 'txid_snapshot_xmax' LANGUAGE C
	IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_snapshot_active(txid_snapshot)
	RETURNS setof bigint
	AS '$libdir/txid', 'txid_snapshot_active' LANGUAGE C
	IMMUTABLE STRICT;


--
-- Special comparision functions used by the remote worker
-- for sync chunk selection
--
CREATE OR REPLACE FUNCTION txid_in_snapshot(bigint, txid_snapshot)
	RETURNS boolean
	AS '$libdir/txid', 'txid_in_snapshot' LANGUAGE C
	IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION txid_not_in_snapshot(bigint, txid_snapshot)
	RETURNS boolean
	AS '$libdir/txid', 'txid_not_in_snapshot' LANGUAGE C
	IMMUTABLE STRICT;

