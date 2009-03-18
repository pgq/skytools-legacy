#ifndef __UTIL_H__
#define __UTIL_H__

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>

typedef uint64_t usec_t;
#define USEC 1000000

usec_t get_cached_time(void);
void reset_time_cache(void);
void fatal_perror(const char *err);

void fatal_noexit(const char *fmt, ...);
void fatal(const char *fmt, ...);
void log_debug(const char *fmt, ...);
void log_error(const char *fmt, ...);

#define Assert(x)

/* broken posix */
static inline void sane_free(const void *p) { free((void*)p); }
#define free sane_free

/* braindead glibc */
#define strlcpy my_strlcpy
#define strlcat my_strlcat
size_t strlcpy(char *dst, const char *src, size_t n);
size_t strlcat(char *dst, const char *src, size_t n);

size_t quote_literal(char *buf, int buflen, const char *src, bool std_quote);

#endif

