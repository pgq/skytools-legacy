/*
 * denytriga.c - Dumb deny trigger.
 *
 * Copyright (c) 2008 Marko Kreen, Skype Technologies OÜ
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <postgres.h>

#include <executor/spi.h>
#include <commands/trigger.h>
#include <utils/memutils.h>

PG_FUNCTION_INFO_V1(pgq_denytriga);
Datum pgq_denytriga(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgq_set_connection_context);
Datum pgq_set_connection_context(PG_FUNCTION_ARGS);

static char *current_context = NULL;

/*
 * Connection context set.
 */

Datum pgq_set_connection_context(PG_FUNCTION_ARGS)
{
	char *ctx;
	if (current_context)
		pfree(current_context);
	current_context = NULL;

	if (PG_NARGS() > 0 && !PG_ARGISNULL(0)) {
		ctx = DatumGetCString(DirectFunctionCall1(textout, PG_GETARG_DATUM(0)));
		current_context = MemoryContextStrdup(TopMemoryContext, ctx);
		pfree(ctx);
	}

	PG_RETURN_VOID();
}

Datum
pgq_denytriga(PG_FUNCTION_ARGS)
{
	TriggerData *tg = (TriggerData *) (fcinfo->context);

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "pgq.denytriga not called as trigger");
	if (!TRIGGER_FIRED_AFTER(tg->tg_event))
		elog(ERROR, "pgq.denytriga must be fired AFTER");
	if (!TRIGGER_FIRED_FOR_ROW(tg->tg_event))
		elog(ERROR, "pgq.denytriga must be fired FOR EACH ROW");

	if (current_context) {
		int i;
		for (i = 0; i < tg->tg_trigger->tgnargs; i++) {
			char *arg = tg->tg_trigger->tgargs[i];
			if (strcmp(arg, current_context) == 0)
				return PointerGetDatum(NULL);
		}
	}
	
	elog(ERROR, "action denied");
}

