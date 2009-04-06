
#include "util.h"

#include <sys/types.h>
#include <sys/time.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>

#include "pgqd.h"

/*
 * Things to test:
 * - Conn per query
 * - show tx
 * - long tx
 * - variable-size query
 */

static usec_t _time_cache;

/*
 * utility functions
 */

static usec_t get_time_usec(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (usec_t)tv.tv_sec * USEC + tv.tv_usec;
}

usec_t get_cached_time(void)
{
	if (!_time_cache)
		_time_cache = get_time_usec();
	return _time_cache;
}
void reset_time_cache(void)
{
	_time_cache = 0;
}

void fatal_perror(const char *err)
{
	log_error("%s: %s", err, strerror(errno));
	exit(1);
}

void fatal_noexit(const char *fmt, ...)
{
	va_list ap;
	char buf[1024];
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	printf("FATAL: %s\n", buf);
}

void fatal(const char *fmt, ...)
{
	va_list ap;
	char buf[1024];
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	printf("FATAL: %s\n", buf);
	exit(1);
}

void log_debug(const char *fmt, ...)
{
	va_list ap;
	char buf[1024];
	if (cf.verbose == 0)
		return;
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	printf("dbg: %s\n", buf);
}

void log_error(const char *fmt, ...)
{
	va_list ap;
	char buf[1024];
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	printf("ERR: %s\n", buf);
}


/*
 * Minimal spec-conforming implementations of strlcpy(), strlcat().
 */

size_t strlcpy(char *dst, const char *src, size_t n)
{
	size_t len = strlen(src);
	if (len < n) {
		memcpy(dst, src, len + 1);
	} else if (n > 0) {
		memcpy(dst, src, n - 1);
		dst[n - 1] = 0;
	}
	return len;
}

size_t strlcat(char *dst, const char *src, size_t n)
{
	size_t pos = 0;
	while (pos < n && dst[pos])
		pos++;
	return pos + strlcpy(dst + pos, src, n - pos);
}

/* SQL quote */
size_t quote_literal(char *buf, int buflen, const char *src, bool std_quote)
{
	char *dst = buf;
	char *end = buf + buflen - 2;

	if (buflen < 3)
		return 0;

	*dst++ = '\'';
	while (*src && dst < end) {
		if (*src == '\'')
			*dst++ = '\'';
		else if (*src == '\\' && !std_quote)
			*dst++ = '\\';
		*dst++ = *src++;
	}
	if (*src || dst > end)
		return 0;

	*dst++ = '\'';
	*dst = 0;

	return dst - buf;
}


