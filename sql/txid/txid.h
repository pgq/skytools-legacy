#ifndef _TXID_H_
#define _TXID_H_

#define MAX_INT64  0x7FFFFFFFFFFFFFFFLL

/* Use unsigned variant internally */
typedef uint64 txid;

typedef struct
{
    int32       __varsz;   /* should not be touched directly */
    uint32      nxip;
    txid xmin;
    txid xmax;
    txid xip[1];
}   TxidSnapshot;

#define TXID_SNAPSHOT_SIZE(nxip) (offsetof(TxidSnapshot, xip) + sizeof(txid) * nxip)

typedef struct {
	uint64		last_value;
	uint64		epoch;
}	TxidEpoch;

/* internal functions */
void	txid_load_epoch(TxidEpoch *state, int try_write);
txid	txid_convert_xid(TransactionId xid, TxidEpoch *state);

/* public functions */
Datum       txid_current(PG_FUNCTION_ARGS);
Datum       txid_current_snapshot(PG_FUNCTION_ARGS);

Datum       txid_snapshot_in(PG_FUNCTION_ARGS);
Datum       txid_snapshot_out(PG_FUNCTION_ARGS);
Datum       txid_snapshot_recv(PG_FUNCTION_ARGS);
Datum       txid_snapshot_send(PG_FUNCTION_ARGS);

Datum       txid_snapshot_xmin(PG_FUNCTION_ARGS);
Datum       txid_snapshot_xmax(PG_FUNCTION_ARGS);
Datum       txid_snapshot_xip(PG_FUNCTION_ARGS);
Datum       txid_visible_in_snapshot(PG_FUNCTION_ARGS);

Datum       txid_snapshot_active(PG_FUNCTION_ARGS);
Datum       txid_in_snapshot(PG_FUNCTION_ARGS);
Datum       txid_not_in_snapshot(PG_FUNCTION_ARGS);


#endif /* _TXID_H_ */

