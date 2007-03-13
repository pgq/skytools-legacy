/*-------------------------------------------------------------------------
 * txid.c
 *
 *	Safe handling of transaction ID's.
 *
 *	Copyright (c) 2003-2004, PostgreSQL Global Development Group
 *	Author: Jan Wieck, Afilias USA INC.
 *
 *	64-bit output: Marko Kreen, Skype Technologies
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "access/xact.h"
#include "funcapi.h"

#include "txid.h"

#ifdef INT64_IS_BUSTED
#error txid needs working int64
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
 * public functions
 */

PG_FUNCTION_INFO_V1(txid_current);
PG_FUNCTION_INFO_V1(txid_snapshot_in);
PG_FUNCTION_INFO_V1(txid_snapshot_out);
PG_FUNCTION_INFO_V1(txid_in_snapshot);
PG_FUNCTION_INFO_V1(txid_not_in_snapshot);
PG_FUNCTION_INFO_V1(txid_current_snapshot);
PG_FUNCTION_INFO_V1(txid_snapshot_xmin);
PG_FUNCTION_INFO_V1(txid_snapshot_xmax);
PG_FUNCTION_INFO_V1(txid_snapshot_active);

/*
 * utility functions
 */

static int _cmp_txid(const void *aa, const void *bb)
{
	const uint64 *a = aa;
	const uint64 *b = bb;
	if (*a < *b)
		return -1;
	if (*a > *b)
		return 1;
	return 0;
}

static void sort_snapshot(TxidSnapshot *snap)
{
	qsort(snap->xip, snap->nxip, sizeof(txid), _cmp_txid);
}

static TxidSnapshot *
parse_snapshot(const char *str)
{
	int	a_size;
	txid *xip;

	int			a_used = 0;
	txid		xmin;
	txid		xmax;
	txid		last_val = 0, val;
	TxidSnapshot *snap;
	int			size;

	char	   *endp;

	a_size = 1024;
	xip = (txid *) palloc(sizeof(txid) * a_size);

	xmin = (txid) strtoull(str, &endp, 0);
	if (*endp != ':')
		elog(ERROR, "illegal txid_snapshot input format");
	str = endp + 1;

	xmax = (txid) strtoull(str, &endp, 0);
	if (*endp != ':')
		elog(ERROR, "illegal txid_snapshot input format");
	str = endp + 1;

	/* it should look sane */
	if (xmin >= xmax || xmin > MAX_INT64 || xmax > MAX_INT64
			|| xmin == 0 || xmax == 0)
		elog(ERROR, "illegal txid_snapshot input format");

	while (*str != '\0')
	{
		if (a_used >= a_size)
		{
			a_size *= 2;
			xip = (txid *) repalloc(xip, sizeof(txid) * a_size);
		}

		/* read next value */
		if (*str == '\'')
		{
			str++;
			val = (txid) strtoull(str, &endp, 0);
			if (*endp != '\'')
				elog(ERROR, "illegal txid_snapshot input format");
			str = endp + 1;
		}
		else
		{
			val = (txid) strtoull(str, &endp, 0);
			str = endp;
		}

		/* require the input to be in order */
		if (val < xmin || val <= last_val || val >= xmax)
			elog(ERROR, "illegal txid_snapshot input format");
		
		xip[a_used++] = val;
		last_val = val;

		if (*str == ',')
			str++;
		else
		{
			if (*str != '\0')
				elog(ERROR, "illegal txid_snapshot input format");
		}
	}

	size = offsetof(TxidSnapshot, xip) + sizeof(txid) * a_used;
	snap = (TxidSnapshot *) palloc(size);
	snap->varsz = size;
	snap->xmin = xmin;
	snap->xmax = xmax;
	snap->nxip = a_used;
	if (a_used > 0)
		memcpy(&(snap->xip[0]), xip, sizeof(txid) * a_used);
	pfree(xip);

	return snap;
}

/*
 * Public functions
 */

/*
 *		txid_current	- Return the current transaction ID as txid
 */
Datum
txid_current(PG_FUNCTION_ARGS)
{
	txid val;
	TxidEpoch state;

	txid_load_epoch(&state, 0);

	val = txid_convert_xid(GetTopTransactionId(), &state);

	PG_RETURN_INT64(val);
}

/*
 * txid_current_snapshot	-	return current snapshot
 */
Datum
txid_current_snapshot(PG_FUNCTION_ARGS)
{
	TxidSnapshot *snap;
	unsigned num, i, size;
	TxidEpoch state;

	if (SerializableSnapshot == NULL)
		elog(ERROR, "get_current_snapshot: SerializableSnapshot == NULL");

	txid_load_epoch(&state, 1);

	num = SerializableSnapshot->xcnt;
	size = offsetof(TxidSnapshot, xip) + sizeof(txid) * num;
	snap = palloc(size);
	snap->varsz = size;
	snap->xmin = txid_convert_xid(SerializableSnapshot->xmin, &state);
	snap->xmax = txid_convert_xid(SerializableSnapshot->xmax, &state);
	snap->nxip = num;
	for (i = 0; i < num; i++)
		snap->xip[i] = txid_convert_xid(SerializableSnapshot->xip[i], &state);

	/* we want then guaranteed ascending order */
	sort_snapshot(snap);

	PG_RETURN_POINTER(snap);
}

/*
 *		txid_snapshot_in	- input function for type txid_snapshot
 */
Datum
txid_snapshot_in(PG_FUNCTION_ARGS)
{
	TxidSnapshot *snap;
	char	   *str = PG_GETARG_CSTRING(0);

	snap = parse_snapshot(str);
	PG_RETURN_POINTER(snap);
}

/*
 *		txid_snapshot_out	- output function for type txid_snapshot
 */
Datum
txid_snapshot_out(PG_FUNCTION_ARGS)
{
	TxidSnapshot *snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(0);

	char	   *str = palloc(60 + snap->nxip * 30);
	char	   *cp = str;
	int			i;

	snprintf(str, 60, "%llu:%llu:",
			(unsigned long long)snap->xmin,
			(unsigned long long)snap->xmax);
	cp = str + strlen(str);

	for (i = 0; i < snap->nxip; i++)
	{
		snprintf(cp, 30, "%llu%s",
				(unsigned long long)snap->xip[i],
				 (i < snap->nxip - 1) ? "," : "");
		cp += strlen(cp);
	}

	PG_RETURN_CSTRING(str);
}


/*
 * txid_in_snapshot	- is txid visible in snapshot ?
 */
Datum
txid_in_snapshot(PG_FUNCTION_ARGS)
{
	txid value = PG_GETARG_INT64(0);
	TxidSnapshot *snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(1);
	int			i;
	int			res = true;

	if (value < snap->xmin)
		res = true;
	else if (value >= snap->xmax)
		res = false;
	else
	{
		for (i = 0; i < snap->nxip; i++)
			if (value == snap->xip[i])
			{
				res = false;
				break;
			}
	}
	PG_FREE_IF_COPY(snap, 1);
	PG_RETURN_BOOL(res);
}


/*
 * txid_not_in_snapshot	- is txid invisible in snapshot ?
 */
Datum
txid_not_in_snapshot(PG_FUNCTION_ARGS)
{
	txid		value = PG_GETARG_INT64(0);
	TxidSnapshot *snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(1);
	int			i;
	int			res = false;

	if (value < snap->xmin)
		res = false;
	else if (value >= snap->xmax)
		res = true;
	else
	{
		for (i = 0; i < snap->nxip; i++)
			if (value == snap->xip[i])
			{
				res = true;
				break;
			}
	}
	PG_FREE_IF_COPY(snap, 1);
	PG_RETURN_BOOL(res);
}

/*
 * txid_snapshot_xmin	-	return snapshot's xmin
 */
Datum
txid_snapshot_xmin(PG_FUNCTION_ARGS)
{
	TxidSnapshot *snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(0);
	txid res = snap->xmin;
	PG_FREE_IF_COPY(snap, 0);
	PG_RETURN_INT64(res);
}

/*
 * txid_snapshot_xmin	-	return snapshot's xmax
 */
Datum
txid_snapshot_xmax(PG_FUNCTION_ARGS)
{
	TxidSnapshot *snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(0);
	txid res = snap->xmax;
	PG_FREE_IF_COPY(snap, 0);
	PG_RETURN_INT64(res);
}

/* remember state between function calls */
struct snap_state {
	int pos;
	TxidSnapshot *snap;
};

/*
 * txid_snapshot_active		- returns uncommitted TXID's in snapshot.
 */
Datum
txid_snapshot_active(PG_FUNCTION_ARGS)
{
	FuncCallContext *fctx;
	struct snap_state *state;

	if (SRF_IS_FIRSTCALL()) {
		TxidSnapshot *snap;
		int statelen;

		snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(0);
		
		fctx = SRF_FIRSTCALL_INIT();
		statelen = sizeof(*state) + snap->varsz;
		state = MemoryContextAlloc(fctx->multi_call_memory_ctx, statelen);
		state->pos = 0;
		state->snap = (TxidSnapshot *)((char *)state + sizeof(*state));
		memcpy(state->snap, snap, snap->varsz);
		fctx->user_fctx = state;

		PG_FREE_IF_COPY(snap, 0);
	}
	fctx = SRF_PERCALL_SETUP();
	state = fctx->user_fctx;
	if (state->pos < state->snap->nxip) {
		Datum res = Int64GetDatum(state->snap->xip[state->pos]);
		state->pos++;
		SRF_RETURN_NEXT(fctx, res);
	} else {
		SRF_RETURN_DONE(fctx);
	}
}

