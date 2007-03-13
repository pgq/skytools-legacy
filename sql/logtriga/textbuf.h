struct TBuf;

typedef struct TBuf TBuf;

TBuf *tbuf_alloc(int start_size);
void tbuf_free(TBuf *tbuf);
int tbuf_get_size(TBuf *tbuf);
void tbuf_reset(TBuf *tbuf);

const text *tbuf_look_text(TBuf *tbuf);
const char *tbuf_look_cstring(TBuf *tbuf);

void tbuf_append_cstring(TBuf *tbuf, const char *str);
void tbuf_append_text(TBuf *tbuf, const text *str);
void tbuf_append_char(TBuf *tbuf, char chr);

text *tbuf_steal_text(TBuf *tbuf);

void tbuf_encode_cstring(TBuf *tbuf,
		const char *str,
		const char *encoding);

void tbuf_encode_data(TBuf *tbuf,
		const uint8 *data, int len,
		const char *encoding);

