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
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"

#include "txid.h"

#ifdef INT64_IS_BUSTED
#error txid needs working int64
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#ifndef SET_VARSIZE
#define SET_VARSIZE(x, len) VARATT_SIZEP(x) = len
#endif

/* txid will be signed int8 in database, so must limit to 63 bits */
#define MAX_TXID   UINT64CONST(0x7FFFFFFFFFFFFFFF)

/*
 * If defined, use bsearch() function for searching
 * txid's inside snapshots that have more than given values.
 */
#define USE_BSEARCH_FOR 100


/*
 * public functions
 */

PG_FUNCTION_INFO_V1(txid_current);
PG_FUNCTION_INFO_V1(txid_snapshot_in);
PG_FUNCTION_INFO_V1(txid_snapshot_out);
PG_FUNCTION_INFO_V1(txid_snapshot_recv);
PG_FUNCTION_INFO_V1(txid_snapshot_send);
PG_FUNCTION_INFO_V1(txid_current_snapshot);
PG_FUNCTION_INFO_V1(txid_snapshot_xmin);
PG_FUNCTION_INFO_V1(txid_snapshot_xmax);

/* new API in 8.3 */
PG_FUNCTION_INFO_V1(txid_visible_in_snapshot);
PG_FUNCTION_INFO_V1(txid_snapshot_xip);

/* old API */
PG_FUNCTION_INFO_V1(txid_in_snapshot);
PG_FUNCTION_INFO_V1(txid_not_in_snapshot);
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

static StringInfo
buf_init(txid xmin, txid xmax)
{
	TxidSnapshot snap;
	StringInfo buf;

	snap.xmin = xmin;
	snap.xmax = xmax;
	snap.nxip = 0;

	buf = makeStringInfo();
	appendBinaryStringInfo(buf, (char *)&snap, offsetof(TxidSnapshot, xip));
	return buf;
}

static void
buf_add_txid(StringInfo buf, txid xid)
{
	TxidSnapshot *snap = (TxidSnapshot *)buf->data;
	snap->nxip++;
	appendBinaryStringInfo(buf, (char *)&xid, sizeof(xid));
}

static TxidSnapshot *
buf_finalize(StringInfo buf)
{
	TxidSnapshot *snap = (TxidSnapshot *)buf->data;
	SET_VARSIZE(snap, buf->len);

	/* buf is not needed anymore */
	buf->data = NULL;
	pfree(buf);

	return snap;
}

static TxidSnapshot *
parse_snapshot(const char *str)
{
	txid		xmin;
	txid		xmax;
	txid		last_val = 0, val;
	char	   *endp;
	StringInfo  buf;

	xmin = (txid) strtoull(str, &endp, 0);
	if (*endp != ':')
		goto bad_format;
	str = endp + 1;

	xmax = (txid) strtoull(str, &endp, 0);
	if (*endp != ':')
		goto bad_format;
	str = endp + 1;

	/* it should look sane */
	if (xmin >= xmax || xmin == 0 || xmax > MAX_INT64)
		goto bad_format;

	/* allocate buffer */
	buf = buf_init(xmin, xmax);

	/* loop over values */
	while (*str != '\0')
	{
		/* read next value */
		val = (txid) strtoull(str, &endp, 0);
		str = endp;

		/* require the input to be in order */
		if (val < xmin || val <= last_val || val >= xmax)
			goto bad_format;
		
		buf_add_txid(buf, val);
		last_val = val;

		if (*str == ',')
			str++;
		else if (*str != '\0')
			goto bad_format;
	}

	return buf_finalize(buf);

bad_format:
	elog(ERROR, "illegal txid_snapshot input format");
	return NULL;
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
	SET_VARSIZE(snap, size);
	snap->xmin = txid_convert_xid(SerializableSnapshot->xmin, &state);
	snap->xmax = txid_convert_xid(SerializableSnapshot->xmax, &state);
	snap->nxip = num;
	for (i = 0; i < num; i++)
		snap->xip[i] = txid_convert_xid(SerializableSnapshot->xip[i], &state);

	/* we want them guaranteed ascending order */
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
	TxidSnapshot   *snap;
	StringInfoData	str;
	int				i;

	snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(0);

	initStringInfo(&str);

	appendStringInfo(&str, "%llu:", (unsigned long long)snap->xmin);
	appendStringInfo(&str, "%llu:", (unsigned long long)snap->xmax);

	for (i = 0; i < snap->nxip; i++)
	{
		appendStringInfo(&str, "%s%llu", ((i > 0) ? "," : ""),
						 (unsigned long long)snap->xip[i]);
	}

	PG_FREE_IF_COPY(snap, 0);

	PG_RETURN_CSTRING(str.data);
}

/*
 * txid_snapshot_recv(internal) returns txid_snapshot
 *
 *		binary input function for type txid_snapshot
 *
 *		format: int4 nxip, int8 xmin, int8 xmax, int8 xip
 */
Datum
txid_snapshot_recv(PG_FUNCTION_ARGS)
{
	StringInfo  buf = (StringInfo) PG_GETARG_POINTER(0);
	TxidSnapshot *snap;
	txid last = 0;
	int nxip;
	int i;
	int avail;
	int expect;
	txid xmin, xmax;

	/*
	 * load nxip and check for nonsense.
	 *
	 * (nxip > avail) check is against int overflows in 'expect'.
	 */
	nxip = pq_getmsgint(buf, 4);
	avail = buf->len - buf->cursor;
	expect = 8 + 8 + nxip * 8;
	if (nxip < 0 || nxip > avail || expect > avail)
		goto bad_format;

	xmin = pq_getmsgint64(buf);
	xmax = pq_getmsgint64(buf);
	if (xmin == 0 || xmax == 0 || xmin > xmax || xmax > MAX_TXID)
		goto bad_format;

	snap = palloc(TXID_SNAPSHOT_SIZE(nxip));
	snap->xmin = xmin;
	snap->xmax = xmax;
	snap->nxip = nxip;
	SET_VARSIZE(snap, TXID_SNAPSHOT_SIZE(nxip));

	for (i = 0; i < nxip; i++)
	{
		txid cur =  pq_getmsgint64(buf);
		if (cur <= last || cur < xmin || cur >= xmax)
			goto bad_format;
		snap->xip[i] = cur;
		last = cur;
	}
	PG_RETURN_POINTER(snap);

bad_format:
	elog(ERROR, "invalid snapshot data");
	return (Datum)NULL;
}

/*
 * txid_snapshot_send(txid_snapshot) returns bytea
 *
 *		binary output function for type txid_snapshot
 *
 *		format: int4 nxip, int8 xmin, int8 xmax, int8 xip
 */
Datum
txid_snapshot_send(PG_FUNCTION_ARGS)
{
	TxidSnapshot *snap = (TxidSnapshot *)PG_GETARG_VARLENA_P(0);
	StringInfoData buf;
	uint32 i;

	pq_begintypsend(&buf);
	pq_sendint(&buf, snap->nxip, 4);
	pq_sendint64(&buf, snap->xmin);
	pq_sendint64(&buf, snap->xmax);
	for (i = 0; i < snap->nxip; i++)
		pq_sendint64(&buf, snap->xip[i]);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


static int
_txid_in_snapshot(txid value, const TxidSnapshot *snap)
{
	if (value < snap->xmin)
		return true;
	else if (value >= snap->xmax)
		return false;
#ifdef USE_BSEARCH_FOR
	else if (snap->nxip >= USE_BSEARCH_FOR)
	{
		void *res;
		res = bsearch(&value, snap->xip, snap->nxip, sizeof(txid), _cmp_txid);
		return (res) ? false : true;
	}
#endif
	else
	{
		int			i;
		for (i = 0; i < snap->nxip; i++)
		{
			if (value == snap->xip[i])
				return false;
		}
		return true;
	}
}

/*
 * txid_in_snapshot	- is txid visible in snapshot ?
 */
Datum
txid_in_snapshot(PG_FUNCTION_ARGS)
{
	txid value = PG_GETARG_INT64(0);
	TxidSnapshot *snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(1);
	int			res;
	
	res = _txid_in_snapshot(value, snap) ? true : false;

	PG_FREE_IF_COPY(snap, 1);
	PG_RETURN_BOOL(res);
}

/*
 * changed api
 */
Datum
txid_visible_in_snapshot(PG_FUNCTION_ARGS)
{
	txid value = PG_GETARG_INT64(0);
	TxidSnapshot *snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(1);
	int			res;
	
	res = _txid_in_snapshot(value, snap) ? true : false;

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
	int			res;

	res = _txid_in_snapshot(value, snap) ? false : true;

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
txid_snapshot_xip(PG_FUNCTION_ARGS)
{
	FuncCallContext *fctx;
	struct snap_state *state;

	if (SRF_IS_FIRSTCALL()) {
		TxidSnapshot *snap;
		int statelen;

		snap = (TxidSnapshot *) PG_GETARG_VARLENA_P(0);
		
		fctx = SRF_FIRSTCALL_INIT();
		statelen = sizeof(*state) + VARSIZE(snap);
		state = MemoryContextAlloc(fctx->multi_call_memory_ctx, statelen);
		state->pos = 0;
		state->snap = (TxidSnapshot *)((char *)state + sizeof(*state));
		memcpy(state->snap, snap, VARSIZE(snap));
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

/* old api */
Datum
txid_snapshot_active(PG_FUNCTION_ARGS)
{
	return txid_snapshot_xip(fcinfo);
}

