
DROP DOMAIN txid;
DROP TYPE txid_snapshot cascade;
DROP SCHEMA txid CASCADE;
DROP FUNCTION get_current_txid();
DROP FUNCTION get_snapshot_xmin();
DROP FUNCTION get_snapshot_xmax();
DROP FUNCTION get_snapshot_active();


