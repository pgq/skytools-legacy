/*-------------------------------------------------------------------------
 * epoch.c
 *
 *	Detect current epoch.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "access/transam.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "catalog/pg_control.h"
#include "access/xlog.h"

#include "txid.h"

/*
 * do a TransactionId -> txid conversion
 */
txid txid_convert_xid(TransactionId xid, TxidEpoch *state)
{
	uint64 epoch;

	/* avoid issues with the the special meaning of 0 */
	if (xid == InvalidTransactionId)
		return MAX_INT64;

	/* return special xid's as-is */
	if (xid < FirstNormalTransactionId)
		return xid;

	/* xid can on both sides on wrap-around */
	epoch = state->epoch;
	if (TransactionIdPrecedes(xid, state->last_value)) {
		if (xid > state->last_value)
			epoch--;
	} else if (TransactionIdFollows(xid, state->last_value)) {
		if (xid < state->last_value)
			epoch++;
	}
	return (epoch << 32) | xid;
}

#if PG_CONTROL_VERSION >= 820

/*
 * PostgreSQl 8.2 keeps track of epoch internally.
 */

void txid_load_epoch(TxidEpoch *state, int try_write)
{
	TransactionId	xid;
	uint32			epoch;

	GetNextXidAndEpoch(&xid, &epoch);

	state->epoch = epoch;
	state->last_value = xid;
}

#else

/*
 * For older PostgreSQL keep epoch in table.
 */

/*
 * this caches the txid_epoch table.
 * The struct should be updated only together with the table.
 */
static TxidEpoch epoch_state = { 0, 0 };

/*
 * load values from txid_epoch table.
 */
static int load_epoch(void)
{
	HeapTuple row;
	TupleDesc rdesc;
	bool isnull = false;
	Datum tmp;
	int res;
	uint64 db_epoch, db_value;

	res = SPI_connect();
	if (res < 0)
		elog(ERROR, "cannot connect to SPI");

	res = SPI_execute("select epoch, last_value from txid.epoch", true, 0);
	if (res != SPI_OK_SELECT)
		elog(ERROR, "load_epoch: select failed?");
	if (SPI_processed != 1)
		elog(ERROR, "load_epoch: there must be exactly 1 row");

	row = SPI_tuptable->vals[0];
	rdesc = SPI_tuptable->tupdesc;

	tmp = SPI_getbinval(row, rdesc, 1, &isnull);
	if (isnull)
		elog(ERROR, "load_epoch: epoch is NULL");
	db_epoch = DatumGetInt64(tmp);

	tmp = SPI_getbinval(row, rdesc, 2, &isnull);
	if (isnull)
		elog(ERROR, "load_epoch: last_value is NULL");
	db_value = DatumGetInt64(tmp);
	
	SPI_finish();

	/*
	 * If the db has lesser values, then some updates were lost.
	 *
	 * Should that be special-cased?  ATM just use db values.
	 * Thus immidiate update.
	 */
	epoch_state.epoch = db_epoch;
	epoch_state.last_value = db_value;
	return 1;
}

/*
 * updates last_value and epoch, if needed
 */
static void save_epoch(void)
{
	int res;
	char qbuf[200];
	uint64 new_epoch, new_value;
	TransactionId xid = GetTopTransactionId();
	TransactionId old_value;

	/* store old state */
	MemoryContext oldcontext = CurrentMemoryContext;
	ResourceOwner oldowner = CurrentResourceOwner;

	/*
	 * avoid changing internal values.
	 */
	new_value = xid;
	new_epoch = epoch_state.epoch;
	old_value = (TransactionId)epoch_state.last_value;
	if (xid < old_value) {
		if (TransactionIdFollows(xid, old_value))
			new_epoch++;
		else
			return;
	}
	sprintf(qbuf, "update txid.epoch set epoch = %llu, last_value = %llu",
				(unsigned long long)new_epoch,
				(unsigned long long)new_value);

	/*
	 * The update may fail in case of SERIALIZABLE transaction.
	 * Try to catch the error and hide it.
	 */
	BeginInternalSubTransaction(NULL);
	PG_TRY();
	{
		/* do the update */
		res = SPI_connect();
		if (res < 0)
			elog(ERROR, "cannot connect to SPI");
		res = SPI_execute(qbuf, false, 0);
		SPI_finish();

		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		/* we expect rollback to clean up inner SPI call */
		RollbackAndReleaseCurrentSubTransaction();
		FlushErrorState();
		res = -1;  /* remember failure */
	}
	PG_END_TRY();

	/* restore old state */
	MemoryContextSwitchTo(oldcontext);
	CurrentResourceOwner = oldowner;

	if (res < 0)
		return;

	/*
	 * Seems the update was successful, update internal state too.
	 *
	 * There is a chance that the TX will be rollbacked, but then
	 * another backend will do the update, or this one at next
	 * checkpoint.
	 */
	epoch_state.epoch = new_epoch;
	epoch_state.last_value = new_value;
}

static void check_epoch(int update_prio)
{
	TransactionId xid = GetTopTransactionId();
	TransactionId recheck, tx_next;
	int ok = 1;

	/* should not happen, but just in case */
	if (xid == InvalidTransactionId)
		return;

	/* new backend */
	if (epoch_state.last_value == 0)
		load_epoch();
	
	/* try to avoid concurrent access */
	if (update_prio)
		recheck = 50000 + 100 * (MyProcPid & 0x1FF);
	else
		recheck = 300000 + 1000 * (MyProcPid & 0x1FF);

	/* read table */
	tx_next = (TransactionId)epoch_state.last_value + recheck;
	if (TransactionIdFollows(xid, tx_next))
		ok = load_epoch();

	/*
	 * check if save is needed.  last_value may be updated above.
	 */
	tx_next = (TransactionId)epoch_state.last_value + recheck;
	if (!ok || TransactionIdFollows(xid, tx_next))
		save_epoch();
}

void txid_load_epoch(TxidEpoch *state, int try_write)
{
	check_epoch(try_write);

	state->epoch = epoch_state.epoch;
	state->last_value = epoch_state.last_value;
}


#endif
