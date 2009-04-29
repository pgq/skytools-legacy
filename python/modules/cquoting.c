/*
 * Fast quoting functions for Python.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <stdbool.h>

#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#endif

#ifdef _MSC_VER
#define inline __inline
#define strcasecmp stricmp
#endif

/*
 * Common buffer management.
 */

struct Buf {
	unsigned char *ptr;
	unsigned long pos;
	unsigned long alloc;
};

static unsigned char *buf_init(struct Buf *buf, unsigned init_size)
{
	if (init_size < 256)
		init_size = 256;
	buf->ptr = PyMem_Malloc(init_size);
	if (buf->ptr) {
		buf->pos = 0;
		buf->alloc = init_size;
	}
	return buf->ptr;
}

/* return new pos */
static unsigned char *buf_enlarge(struct Buf *buf, unsigned need_room)
{
	unsigned alloc = buf->alloc;
	unsigned need_size = buf->pos + need_room;
	unsigned char *ptr;

	/* no alloc needed */
	if (need_size < alloc)
		return buf->ptr + buf->pos;

	if (alloc <= need_size / 2)
		alloc = need_size;
	else
		alloc = alloc * 2;

	ptr = PyMem_Realloc(buf->ptr, alloc);
	if (!ptr)
		return NULL;

	buf->ptr = ptr;
	buf->alloc = alloc;
	return buf->ptr + buf->pos;
}

static void buf_free(struct Buf *buf)
{
	PyMem_Free(buf->ptr);
	buf->ptr = NULL;
	buf->pos = buf->alloc = 0;
}

static inline unsigned char *buf_get_target_for(struct Buf *buf, unsigned len)
{
	if (buf->pos + len <= buf->alloc)
		return buf->ptr + buf->pos;
	else
		return buf_enlarge(buf, len);
}

static inline void buf_set_target(struct Buf *buf, unsigned char *newpos)
{
	assert(buf->ptr + buf->pos <= newpos);
	assert(buf->ptr + buf->alloc >= newpos);

	buf->pos = newpos - buf->ptr;
}

static inline int buf_put(struct Buf *buf, unsigned char c)
{
	if (buf->pos < buf->alloc) {
		buf->ptr[buf->pos++] = c;
		return 1;
	} else if (buf_enlarge(buf, 1)) {
		buf->ptr[buf->pos++] = c;
		return 1;
	}
	return 0;
}

static PyObject *buf_pystr(struct Buf *buf, unsigned start_pos, unsigned char *newpos)
{
	PyObject *res;
	if (newpos)
		buf_set_target(buf, newpos);
	res = PyString_FromStringAndSize((char *)buf->ptr + start_pos, buf->pos - start_pos);
	buf_free(buf);
	return res;
}

/*
 * Get string data
 */

static Py_ssize_t get_buffer(PyObject *obj, unsigned char **buf_p, PyObject **tmp_obj_p)
{
	PyBufferProcs *bfp;
	PyObject *str = NULL;
	Py_ssize_t res;

	/* check for None */
	if (obj == Py_None) {
		PyErr_Format(PyExc_TypeError, "None is not allowed here");
		return -1;
	}

	/* is string or unicode ? */
	if (PyString_Check(obj) || PyUnicode_Check(obj)) {
		if (PyString_AsStringAndSize(obj, (char**)buf_p, &res) < 0)
			return -1;
		return res;
	}

	/* try to get buffer */
	bfp = obj->ob_type->tp_as_buffer;
	if (bfp && bfp->bf_getsegcount && bfp->bf_getreadbuffer) {
		if (bfp->bf_getsegcount(obj, NULL) == 1)
			return bfp->bf_getreadbuffer(obj, 0, (void**)buf_p);
	}

	/*
	 * Not a string-like object, run str() or it.
	 */

	/* are we in recursion? */
	if (tmp_obj_p == NULL) {
		PyErr_Format(PyExc_TypeError, "Cannot convert to string - get_buffer() recusively failed");
		return -1;
	}

	/* do str() then */
	str = PyObject_Str(obj);
	res = -1;
	if (str != NULL) {
		res = get_buffer(str, buf_p, NULL);
		if (res >= 0) {
			*tmp_obj_p = str;
		} else {
			Py_CLEAR(str);
		}
	}
	return res;
}

/*
 * Common argument parsing.
 */

typedef PyObject *(*quote_fn)(unsigned char *src, Py_ssize_t src_len);

static PyObject *common_quote(PyObject *args, quote_fn qfunc)
{
	unsigned char *src = NULL;
        Py_ssize_t src_len = 0;
	PyObject *arg, *res, *strtmp = NULL;
        if (!PyArg_ParseTuple(args, "O", &arg))
                return NULL;
	if (arg != Py_None) {
		src_len = get_buffer(arg, &src, &strtmp);
		if (src_len < 0)
			return NULL;
	}
	res = qfunc(src, src_len);
	Py_CLEAR(strtmp);
	return res;
}

/*
 * Simple quoting functions.
 */

static const char doc_quote_literal[] =
"Quote a literal value for SQL.\n"
"\n"
"If string contains '\\', it is quoted and result is prefixed with E.\n"
"Input value of None results in string \"null\" without quotes.\n"
"\n"
"C implementation.\n";

static PyObject *quote_literal_body(unsigned char *src, Py_ssize_t src_len)
{
	struct Buf buf;
	unsigned char *esc, *dst, *src_end = src + src_len;
	unsigned int start_ofs = 1;

	if (src == NULL)
		return PyString_FromString("null");

	esc = dst = buf_init(&buf, src_len * 2 + 2 + 1);
        if (!dst)
		return NULL;

	*dst++ = ' ';
	*dst++ = '\'';
        while (src < src_end) {
		if (*src == '\\') {
			*dst++ = '\\';
			start_ofs = 0;
		} else if (*src == '\'') {
			*dst++ = '\'';
		}
		*dst++ = *src++;
        }
	*dst++ = '\'';
	if (start_ofs == 0)
		*esc = 'E';
	return buf_pystr(&buf, start_ofs, dst);
}

static PyObject *quote_literal(PyObject *self, PyObject *args)
{
	return common_quote(args, quote_literal_body);
}

/* COPY field */
static const char doc_quote_copy[] =
"Quoting for COPY data.  None is converted to \\N.\n\n"
"C implementation.";

static PyObject *quote_copy_body(unsigned char *src, Py_ssize_t src_len)
{
	unsigned char *dst, *src_end = src + src_len;
	struct Buf buf;

	if (src == NULL)
		return PyString_FromString("\\N");

	dst = buf_init(&buf, src_len * 2);
        if (!dst)
		return NULL;

        while (src < src_end) {
		switch (*src) {
		case '\t': *dst++ = '\\'; *dst++ = 't'; src++; break;
		case '\n': *dst++ = '\\'; *dst++ = 'n'; src++; break;
		case '\r': *dst++ = '\\'; *dst++ = 'r'; src++; break;
		case '\\': *dst++ = '\\'; *dst++ = '\\'; src++; break;
		default: *dst++ = *src++; break;
		}
        }
	return buf_pystr(&buf, 0, dst);
}

static PyObject *quote_copy(PyObject *self, PyObject *args)
{
	return common_quote(args, quote_copy_body);
}

/* raw bytea for byteain() */
static const char doc_quote_bytea_raw[] =
"Quoting for bytea parser.  Returns None as None.\n"
"\n"
"C implementation.";

static PyObject *quote_bytea_raw_body(unsigned char *src, Py_ssize_t src_len)
{
	unsigned char *dst, *src_end = src + src_len;
	struct Buf buf;

	if (src == NULL) {
		Py_INCREF(Py_None);
		return Py_None;
	}

	dst = buf_init(&buf, src_len * 4);
        if (!dst)
		return NULL;

        while (src < src_end) {
		if (*src < 0x20 || *src >= 0x7F) {
			*dst++ = '\\';
			*dst++ = '0' + (*src >> 6);
			*dst++ = '0' + ((*src >> 3) & 7);
			*dst++ = '0' + (*src & 7);
			src++;
		} else {
			if (*src == '\\')
				*dst++ = '\\';
			*dst++ = *src++;
		}
        }
	return buf_pystr(&buf, 0, dst);
}

static PyObject *quote_bytea_raw(PyObject *self, PyObject *args)
{
	return common_quote(args, quote_bytea_raw_body);
}

/* SQL unquote */
static const char doc_unquote_literal[] =
"Unquote SQL value.\n\n"
"E'..' -> extended quoting.\n"
"'..' -> standard or extended quoting\n"
"null -> None\n"
"other -> returned as-is\n\n"
"C implementation.\n";

static PyObject *do_sql_ext(unsigned char *src, Py_ssize_t src_len)
{
	unsigned char *dst, *src_end = src + src_len;
	struct Buf buf;

	dst = buf_init(&buf, src_len);
        if (!dst)
		return NULL;

        while (src < src_end) {
		if (*src == '\'') {
			src++;
			if (src < src_end && *src == '\'') {
				*dst++ = *src++;
				continue;
			}
			goto failed;
		}
		if (*src != '\\') {
			*dst++ = *src++;
			continue;
		}
		if (++src >= src_end)
			goto failed;
		switch (*src) {
		case 't': *dst++ = '\t'; src++; break;
		case 'n': *dst++ = '\n'; src++; break;
		case 'r': *dst++ = '\r'; src++; break;
		case 'a': *dst++ = '\a'; src++; break;
		case 'b': *dst++ = '\b'; src++; break;
		default:
			if (*src >= '0' && *src <= '7') {
				unsigned char c = *src++ - '0';
				if (src < src_end && *src >= '0' && *src <= '7') {
					c = (c << 3) | ((*src++) - '0');
					if (src < src_end && *src >= '0' && *src <= '7')
						c = (c << 3) | ((*src++) - '0');
				}
				*dst++ = c;
			} else {
				*dst++ = *src++;
			}
		}
        }
	return buf_pystr(&buf, 0, dst);
failed:
	PyErr_Format(PyExc_ValueError, "Broken exteded SQL string");
	return NULL;
}

static PyObject *do_sql_std(unsigned char *src, Py_ssize_t src_len)
{
	unsigned char *dst, *src_end = src + src_len;
	struct Buf buf;

	dst = buf_init(&buf, src_len);
        if (!dst)
		return NULL;

        while (src < src_end) {
		if (*src != '\'') {
			*dst++ = *src++;
			continue;
		}
		src++;
		if (src >= src_end || *src != '\'')
			goto failed;
		*dst++ = *src++;
        }
	return buf_pystr(&buf, 0, dst);
failed:
	PyErr_Format(PyExc_ValueError, "Broken standard SQL string");
	return NULL;
}

static PyObject *do_dolq(unsigned char *src, Py_ssize_t src_len)
{
	/* src_len >= 2, '$' in start and end */
	unsigned char *src_end = src + src_len;
	unsigned char *p1 = src + 1, *p2 = src_end - 2;

	while (p1 < src_end && *p1 != '$')
		p1++;
	while (p2 > src && *p2 != '$')
		p2--;
	if (p2 <= p1)
		goto failed;

	p1++; /* position after '$' */

	if ((p1 - src) != (src_end - p2))
		goto failed;
	if (memcmp(src, p2, p1 - src) != 0)
		goto failed;

	return PyString_FromStringAndSize((char *)p1, p2 - p1);

failed:
	PyErr_Format(PyExc_ValueError, "Broken dollar-quoted string");
	return NULL;
}

static PyObject *unquote_literal(PyObject *self, PyObject *args)
{
	unsigned char *src = NULL;
        Py_ssize_t src_len = 0;
	int stdstr = 0;
	PyObject *value = NULL;
        if (!PyArg_ParseTuple(args, "O|i", &value, &stdstr))
                return NULL;
	if (PyString_AsStringAndSize(value, (char **)&src, &src_len) < 0)
		return NULL;
	if (src_len == 4 && strcasecmp((char *)src, "null") == 0) {
		Py_INCREF(Py_None);
		return Py_None;
	}
	if (src_len >= 2 && src[0] == '$' && src[src_len - 1] == '$')
		return do_dolq(src, src_len);
	if (src_len < 2 || src[src_len - 1] != '\'')
		goto badstr;
	if (src[0] == '\'') {
		src++; src_len -= 2;
		return stdstr ? do_sql_std(src, src_len) : do_sql_ext(src, src_len);
	} else if (src_len > 2 && (src[0] | 0x20) == 'e' && src[1] == '\'') {
		src += 2; src_len -= 3;
		return do_sql_ext(src, src_len);
	}
badstr:
	Py_INCREF(value);
	return value;
}

/* C unescape */
static const char doc_unescape[] =
"Unescape C-style escaped string.\n\n"
"C implementation.";

static PyObject *unescape_body(unsigned char *src, Py_ssize_t src_len)
{
	unsigned char *dst, *src_end = src + src_len;
	struct Buf buf;

	if (src == NULL) {
		PyErr_Format(PyExc_TypeError, "None not allowed");
		return NULL;
	}

	dst = buf_init(&buf, src_len);
        if (!dst)
		return NULL;

        while (src < src_end) {
		if (*src != '\\') {
			*dst++ = *src++;
			continue;
		}
		if (++src >= src_end)
			goto failed;
		switch (*src) {
		case 't': *dst++ = '\t'; src++; break;
		case 'n': *dst++ = '\n'; src++; break;
		case 'r': *dst++ = '\r'; src++; break;
		case 'a': *dst++ = '\a'; src++; break;
		case 'b': *dst++ = '\b'; src++; break;
		default:
			if (*src >= '0' && *src <= '7') {
				unsigned char c = *src++ - '0';
				if (src < src_end && *src >= '0' && *src <= '7') {
					c = (c << 3) | ((*src++) - '0');
					if (src < src_end && *src >= '0' && *src <= '7')
						c = (c << 3) | ((*src++) - '0');
				}
				*dst++ = c;
			} else {
				*dst++ = *src++;
			}
		}
        }
	return buf_pystr(&buf, 0, dst);
failed:
	PyErr_Format(PyExc_ValueError, "Broken string - \\ at the end");
	return NULL;
}

static PyObject *unescape(PyObject *self, PyObject *args)
{
	return common_quote(args, unescape_body);
}

/*
 * urlencode of dict
 */

static bool urlenc(struct Buf *buf, PyObject *obj)
{
	Py_ssize_t len;
	unsigned char *src, *dst;
	PyObject *strtmp = NULL;
	static const unsigned char hextbl[] = "0123456789abcdef";
	bool ok = false;

	len = get_buffer(obj, &src, &strtmp);
	if (len < 0)
		goto failed;

	dst = buf_get_target_for(buf, len * 3);
	if (!dst)
		goto failed;

	while (len--) {
		if ((*src >= 'a' && *src <= 'z') ||
		    (*src >= 'A' && *src <= 'Z') ||
		    (*src >= '0' && *src <= '9') ||
		    (*src == '.' || *src == '_' || *src == '-'))
		{
			*dst++ = *src++;
		} else if (*src == ' ') {
			*dst++ = '+'; src++;
		} else {
			*dst++ = '%';
			*dst++ = hextbl[*src >> 4];
			*dst++ = hextbl[*src & 0xF];
			src++;
		}
	}
	buf_set_target(buf, dst);
	ok = true;
failed:
	Py_CLEAR(strtmp);
	return ok;
}

/* urlencode key+val pair.  val can be None */
static bool urlenc_keyval(struct Buf *buf, PyObject *key, PyObject *value, bool needAmp)
{
	if (needAmp && !buf_put(buf, '&'))
		return false;
	if (!urlenc(buf, key))
		return false;
	if (value != Py_None) {
		if (!buf_put(buf, '='))
			return false;
		if (!urlenc(buf, value))
			return false;
	}
	return true;
}

/* encode native dict using PyDict_Next */
static PyObject *encode_dict(PyObject *data)
{
	PyObject *key, *value;
	Py_ssize_t pos = 0;
	bool needAmp = false;
	struct Buf buf;
	if (!buf_init(&buf, 1024))
		return NULL;
	while (PyDict_Next(data, &pos, &key, &value)) {
		if (!urlenc_keyval(&buf, key, value, needAmp))
			goto failed;
		needAmp = true;
	}
	return buf_pystr(&buf, 0, NULL);
failed:
	buf_free(&buf);
	return NULL;
}

/* encode custom object using .iteritems() */
static PyObject *encode_dictlike(PyObject *data)
{
	PyObject *key = NULL, *value = NULL, *tup, *iter;
	struct Buf buf;
	bool needAmp = false;

	if (!buf_init(&buf, 1024))
		return NULL;

	iter = PyObject_CallMethod(data, "iteritems", NULL);
	if (iter == NULL) {
		buf_free(&buf);
		return NULL;
	}

	while ((tup = PyIter_Next(iter))) {
		key = PySequence_GetItem(tup, 0);
		value = key ? PySequence_GetItem(tup, 1) : NULL;
		Py_CLEAR(tup);
		if (!key || !value)
			goto failed;

		if (!urlenc_keyval(&buf, key, value, needAmp))
			goto failed;
		needAmp = true;

		Py_CLEAR(key);
		Py_CLEAR(value);
	}
	/* allow error from iterator */
	if (PyErr_Occurred())
		goto failed;

	Py_CLEAR(iter);
	return buf_pystr(&buf, 0, NULL);
failed:
	buf_free(&buf);
	Py_CLEAR(iter);
	Py_CLEAR(key);
	Py_CLEAR(value);
	return NULL;
}

static const char doc_db_urlencode[] =
"Urlencode for database records.\n"
"If a value is None the key is output without '='.\n"
"\n"
"C implementation.";

static PyObject *db_urlencode(PyObject *self, PyObject *args)
{
	PyObject *data;
        if (!PyArg_ParseTuple(args, "O", &data))
                return NULL;
	if (PyDict_Check(data)) {
		return encode_dict(data);
	} else {
		return encode_dictlike(data);
	}
}

/*
 * urldecode to dict
 */

static inline int gethex(unsigned char c)
{
	if (c >= '0' && c <= '9') return c - '0';
	c |= 0x20;
	if (c >= 'a' && c <= 'f') return c - 'a' + 10;
	return -1;
}

static PyObject *get_elem(unsigned char *buf, unsigned char **src_p, unsigned char *src_end)
{
	int c1, c2;
	unsigned char *src = *src_p;
	unsigned char *dst = buf;

	while (src < src_end) {
		switch (*src) {
		case '%':
			if (++src + 2 > src_end)
				goto hex_incomplete;
                        if ((c1 = gethex(*src++)) < 0)
				goto hex_invalid;
                        if ((c2 = gethex(*src++)) < 0)
				goto hex_invalid;
			*dst++ = (c1 << 4) | c2;
			break;
		case '+':
			*dst++ = ' '; src++;
			break;
		case '&':
		case '=':
			goto gotit;
		default:
			*dst++ = *src++;
		}
	}
gotit:
	*src_p = src;
	return PyString_FromStringAndSize((char *)buf, dst - buf);

hex_incomplete:
	PyErr_Format(PyExc_ValueError, "Incomplete hex code");
	return NULL;
hex_invalid:
	PyErr_Format(PyExc_ValueError, "Invalid hex code");
	return NULL;
}

static const char doc_db_urldecode[] =
"Urldecode from string to dict.\n"
"NULL are detected by missing '='.\n"
"Duplicate keys are ignored - only latest is kept.\n"
"\n"
"C implementation.";

static PyObject *db_urldecode(PyObject *self, PyObject *args)
{
	unsigned char *src, *src_end;
	Py_ssize_t src_len;
	PyObject *dict = NULL, *key = NULL, *value = NULL;
	struct Buf buf;

        if (!PyArg_ParseTuple(args, "t#", &src, &src_len))
                return NULL;
	if (!buf_init(&buf, src_len))
		return NULL;

	dict = PyDict_New();
	if (!dict) {
		buf_free(&buf);
		return NULL;
	}

	src_end = src + src_len;
	while (src < src_end) {
                if (*src == '&') {
                    src++;
                    continue;
                }

		key = get_elem(buf.ptr, &src, src_end);
		if (!key)
			goto failed;

		if (src < src_end && *src == '=') {
			src++;
			value = get_elem(buf.ptr, &src, src_end);
			if (value == NULL)
				goto failed;
		} else {
			Py_INCREF(Py_None);
			value = Py_None;
		}

		/* lessen memory usage by intering */
		PyString_InternInPlace(&key);

		if (PyDict_SetItem(dict, key, value) < 0)
			goto failed;
		Py_CLEAR(key);
		Py_CLEAR(value);
	}
	buf_free(&buf);
	return dict;
failed:
	buf_free(&buf);
	Py_CLEAR(key);
	Py_CLEAR(value);
	Py_CLEAR(dict);
	return NULL;
}

/*
 * Module initialization
 */

static PyMethodDef
cquoting_methods[] = {
	{ "quote_literal", quote_literal, METH_VARARGS, doc_quote_literal },
	{ "quote_copy", quote_copy, METH_VARARGS, doc_quote_copy },
	{ "quote_bytea_raw", quote_bytea_raw, METH_VARARGS, doc_quote_bytea_raw },
	{ "unescape", unescape, METH_VARARGS, doc_unescape },
	{ "db_urlencode", db_urlencode, METH_VARARGS, doc_db_urlencode },
	{ "db_urldecode", db_urldecode, METH_VARARGS, doc_db_urldecode },
	{ "unquote_literal", unquote_literal, METH_VARARGS, doc_unquote_literal },
	{ NULL }
};

PyMODINIT_FUNC
init_cquoting(void)
{
	PyObject *module;
	module = Py_InitModule("_cquoting", cquoting_methods);
	PyModule_AddStringConstant(module, "__doc__", "fast quoting for skytools");
}

