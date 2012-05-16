
#include <postgres.h>

#include "utils/elog.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "parser/keywords.h"

#if 1
#define talloc(len)        malloc(len)
#define trealloc(p, len)   realloc(p, len)
#define tfree(p)           free(p)
#else
#define talloc(len)        palloc(len)
#define trealloc(p, len)   repalloc(p, len)
#define tfree(p)           pfree(p)
#endif

#include "textbuf.h"

#ifndef SET_VARSIZE
#define SET_VARSIZE(x, len) VARATT_SIZEP(x) = (len)
#endif

struct TBuf {
	text *data;
	int size;
};

static void request_avail(TBuf *tbuf, int len)
{
	int newlen = tbuf->size;
	int need = VARSIZE(tbuf->data) + len;
	if (need < newlen)
		return;
	while (need > newlen)
		newlen *= 2;
	tbuf->data = trealloc(tbuf->data, newlen);
	tbuf->size = newlen;
}

static inline char *get_endp(TBuf *tbuf)
{
	char *p = VARDATA(tbuf->data);
	int len = VARSIZE(tbuf->data) - VARHDRSZ;
	return p + len;
}

static inline void inc_used(TBuf *tbuf, int len)
{
	SET_VARSIZE(tbuf->data, VARSIZE(tbuf->data) + len);
}

static void tbuf_init(TBuf *tbuf, int start_size)
{
	if (start_size < VARHDRSZ)
		start_size = VARHDRSZ;
	tbuf->data = talloc(start_size);
	tbuf->size = start_size;
	SET_VARSIZE(tbuf->data, VARHDRSZ);
}

TBuf *tbuf_alloc(int start_size)
{
	TBuf *res;
	res = talloc(sizeof(TBuf));
	tbuf_init(res, start_size);
	return res;
}

void tbuf_free(TBuf *tbuf)
{
	if (tbuf->data)
		tfree(tbuf->data);
	tfree(tbuf);
}

int tbuf_get_size(TBuf *tbuf)
{
	return VARSIZE(tbuf->data) - VARHDRSZ;
}

void tbuf_reset(TBuf *tbuf)
{
	SET_VARSIZE(tbuf->data, VARHDRSZ);
}

const text *tbuf_look_text(TBuf *tbuf)
{
	return tbuf->data;
}

const char *tbuf_look_cstring(TBuf *tbuf)
{
	char *p;
	request_avail(tbuf, 1);
	p = get_endp(tbuf);
	*p = 0;
	return VARDATA(tbuf->data);
}

void tbuf_append_cstring(TBuf *tbuf, const char *str)
{
	int len = strlen(str);
	request_avail(tbuf, len);
	memcpy(get_endp(tbuf), str, len);
	inc_used(tbuf, len);
}

void tbuf_append_text(TBuf *tbuf, const text *str)
{
	int len = VARSIZE(str) - VARHDRSZ;
	request_avail(tbuf, len);
	memcpy(get_endp(tbuf), VARDATA(str), len);
	inc_used(tbuf, len);
}

void tbuf_append_char(TBuf *tbuf, char chr)
{
	char *p;
	request_avail(tbuf, 1);
	p = get_endp(tbuf);
	*p = chr;
	inc_used(tbuf, 1);
}

text *tbuf_steal_text(TBuf *tbuf)
{
	text *data = tbuf->data;
	tbuf->data = NULL;
	return data;
}

static const char b64tbl[] =
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static int b64encode(char *dst, const uint8 *src, int srclen)
{
	char       *p = dst;
	const uint8 *s = src, *end = src + srclen;
	int         pos = 2;
	uint32      buf = 0;

	while (s < end) {
		buf |= (unsigned char) *s << (pos << 3);
		pos--;
		s++;
		/* write it out */
		if (pos < 0) {
			*p++ = b64tbl[ (buf >> 18) & 0x3f ];
			*p++ = b64tbl[ (buf >> 12) & 0x3f ];
			*p++ = b64tbl[ (buf >> 6) & 0x3f ];
			*p++ = b64tbl[ buf & 0x3f ];
			pos = 2;
			buf = 0;
		}
	}
	if (pos != 2) {
		*p++ = b64tbl[ (buf >> 18) & 0x3f ];
		*p++ = b64tbl[ (buf >> 12) & 0x3f ];
		*p++ = (pos == 0) ? b64tbl[ (buf >> 6) & 0x3f ] : '=';
		*p++ = '=';
	}
	return p - dst;
}

static const char hextbl[] = "0123456789abcdef";
static int urlencode(char *dst, const uint8 *src, int srclen)
{
	const uint8 *end = src + srclen;
	char *p = dst;
	while (src < end) {
		if (*src == '=')
			*p++ = '+';
		else if ((*src >= '0' && *src <= '9')
			|| (*src >= 'A' && *src <= 'Z')
			|| (*src >= 'a' && *src <= 'z'))
			*p++ = *src;
		else {
			*p++ = '%';
			*p++ = hextbl[*src >> 4];
			*p++ = hextbl[*src & 15];
		}
	}
	return p - dst;
}

static int quote_literal(char *dst, const uint8 *src, int srclen)
{
	const uint8 *cp1;
	char	   *cp2;
	int			wl;
	bool		is_ext = false;

	cp1 = src;
	cp2 = dst;

	*cp2++ = '\'';
	while (srclen > 0)
	{
		if ((wl = pg_mblen((const char *)cp1)) != 1)
		{
			if (wl > srclen)
				wl = srclen;
			srclen -= wl;

			while (wl-- > 0)
				*cp2++ = *cp1++;
			continue;
		}

		if (*cp1 == '\'') {
			*cp2++ = '\'';
		} else if (*cp1 == '\\') {
			if (!is_ext) {
				memmove(dst + 1, dst, cp2 - dst);
				cp2++;
				is_ext = true;
				*dst = 'E';
			}
			*cp2++ = '\\';
		}
		*cp2++ = *cp1++;
		srclen--;
	}

	*cp2++ = '\'';

	return cp2 - dst;
}

/* check if ident is keyword that needs quoting */
static bool is_keyword(const char *ident)
{
	const ScanKeyword *kw;

	/* do the lookup */
#if PG_VERSION_NUM >= 80500
	kw = ScanKeywordLookup(ident, ScanKeywords, NumScanKeywords);
#else
	kw = ScanKeywordLookup(ident);
#endif

	/* unreserved? */
#if PG_VERSION_NUM >= 80300
	if (kw && kw->category == UNRESERVED_KEYWORD)
		return false;
#endif

	/* found anything? */
	return kw != NULL;
}

/*
 * slon_quote_identifier                     - Quote an identifier only if needed
 *
 * When quotes are needed, we palloc the required space; slightly
 * space-wasteful but well worth it for notational simplicity.
 *
 * Version: pgsql/src/backend/utils/adt/ruleutils.c,v 1.188 2005/01/13 17:19:10
 */
static int
quote_ident(char *dst, const uint8 *src, int srclen)
{
        /*
         * Can avoid quoting if ident starts with a lowercase letter or
         * underscore and contains only lowercase letters, digits, and
         * underscores, *and* is not any SQL keyword.  Otherwise, supply
         * quotes.
         */
        int                     nquotes = 0;
        bool            safe;
        const char *ptr;
        char       *optr;
		char ident[NAMEDATALEN + 1];

		/* expect idents be not bigger than NAMEDATALEN */
		if (srclen > NAMEDATALEN)
			srclen = NAMEDATALEN;
		memcpy(ident, src, srclen);
		ident[srclen] = 0;

        /*
         * would like to use <ctype.h> macros here, but they might yield
         * unwanted locale-specific results...
         */
        safe = ((ident[0] >= 'a' && ident[0] <= 'z') || ident[0] == '_');

        for (ptr = ident; *ptr; ptr++)
        {
                char            ch = *ptr;

                if ((ch >= 'a' && ch <= 'z') ||
                        (ch >= '0' && ch <= '9') ||
                        (ch == '_'))
                        continue; /* okay */

                safe = false;
                if (ch == '"')
                        nquotes++;
        }

        if (safe) {
            if (is_keyword(ident))
                safe = false;
        }

		optr = dst;
		if (!safe)
				*optr++ = '"';

        for (ptr = ident; *ptr; ptr++)
        {
                char            ch = *ptr;

                if (ch == '"')
                        *optr++ = '"';
                *optr++ = ch;
        }
		if (!safe)
				*optr++ = '"';

        return optr - dst;
}


void tbuf_encode_cstring(TBuf *tbuf,
		const char *str,
		const char *encoding)
{
	if (str == NULL)
		elog(ERROR, "tbuf_encode_cstring: NULL");
	tbuf_encode_data(tbuf, (const uint8 *)str, strlen(str), encoding);
}

void tbuf_encode_data(TBuf *tbuf,
		const uint8 *data, int len,
		const char *encoding)
{
	int dlen = 0;
	char *dst;
	if (strcmp(encoding, "url") == 0) {
		request_avail(tbuf, len*3);
		dst = get_endp(tbuf);
		dlen = urlencode(dst, data, len);
	} else if (strcmp(encoding, "base64") == 0) {
		request_avail(tbuf, (len + 2) * 4 / 3);
		dst = get_endp(tbuf);
		dlen = b64encode(dst, data, len);
	} else if (strcmp(encoding, "quote_literal") == 0) {
		request_avail(tbuf, len * 2 + 2);
		dst = get_endp(tbuf);
		dlen = quote_literal(dst, data, len);
	} else if (strcmp(encoding, "quote_ident") == 0) {
		request_avail(tbuf, len * 2 + 2);
		dst = get_endp(tbuf);
		dlen = quote_ident(dst, data, len);
	} else
		elog(ERROR, "bad encoding");
	inc_used(tbuf, dlen);
}

