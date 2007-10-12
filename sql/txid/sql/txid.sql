-- init
\set ECHO none
\i txid.sql
\set ECHO all

-- i/o
select '12:13:'::txid_snapshot;
select '12:13:1,2'::txid_snapshot;

-- errors
select '31:12:'::txid_snapshot;
select '0:1:'::txid_snapshot;
select '12:13:0'::txid_snapshot;
select '12:13:2,1'::txid_snapshot;

create table snapshot_test (
	nr	integer,
	snap	txid_snapshot
);

insert into snapshot_test values (1, '12:13:');
insert into snapshot_test values (2, '12:20:13,15,18');
insert into snapshot_test values (3, '100001:100009:100005,100007,100008');

select snap from snapshot_test order by nr;

select  txid_snapshot_xmin(snap),
	txid_snapshot_xmax(snap),
	txid_snapshot_xip(snap)
from snapshot_test order by nr;

select id, txid_visible_in_snapshot(id, snap)
from snapshot_test, generate_series(11, 21) id
where nr = 2;

-- test current values also
select txid_current() >= txid_snapshot_xmin(txid_current_snapshot());
-- select txid_current_txid() < txid_snapshot_xmax(txid_current_snapshot());

-- select txid_in_snapshot(txid_current_txid(), txid_current_snapshot()),
--    txid_not_in_snapshot(txid_current_txid(), txid_current_snapshot());

