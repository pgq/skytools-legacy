/*
 * Postgres hashes for Python.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#endif

#include <stdint.h>
#include <string.h>


typedef uint32_t (*hash_fn_t)(const void *src, unsigned src_len);

typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;

#define rot(x, k) (((x)<<(k)) | ((x)>>(32-(k))))

/*
 * Old Postgres hashtext()
 */

#define mix_old(a,b,c) \
{ \
  a -= b; a -= c; a ^= ((c)>>13); \
  b -= c; b -= a; b ^= ((a)<<8); \
  c -= a; c -= b; c ^= ((b)>>13); \
  a -= b; a -= c; a ^= ((c)>>12);  \
  b -= c; b -= a; b ^= ((a)<<16); \
  c -= a; c -= b; c ^= ((b)>>5); \
  a -= b; a -= c; a ^= ((c)>>3);	\
  b -= c; b -= a; b ^= ((a)<<10); \
  c -= a; c -= b; c ^= ((b)>>15); \
}

static uint32_t hash_old_hashtext(const void *_k, unsigned keylen)
{
	const unsigned char *k = _k;
	register uint32 a, b, c, len;

	/* Set up the internal state */
	len = keylen;
	a = b = 0x9e3779b9;			/* the golden ratio; an arbitrary value */
	c = 3923095;				/* initialize with an arbitrary value */

	/* handle most of the key */
	while (len >= 12)
	{
		a += (k[0] + ((uint32) k[1] << 8) + ((uint32) k[2] << 16) + ((uint32) k[3] << 24));
		b += (k[4] + ((uint32) k[5] << 8) + ((uint32) k[6] << 16) + ((uint32) k[7] << 24));
		c += (k[8] + ((uint32) k[9] << 8) + ((uint32) k[10] << 16) + ((uint32) k[11] << 24));
		mix_old(a, b, c);
		k += 12;
		len -= 12;
	}

	/* handle the last 11 bytes */
	c += keylen;
	switch (len)				/* all the case statements fall through */
	{
		case 11:
			c += ((uint32) k[10] << 24);
		case 10:
			c += ((uint32) k[9] << 16);
		case 9:
			c += ((uint32) k[8] << 8);
			/* the first byte of c is reserved for the length */
		case 8:
			b += ((uint32) k[7] << 24);
		case 7:
			b += ((uint32) k[6] << 16);
		case 6:
			b += ((uint32) k[5] << 8);
		case 5:
			b += k[4];
		case 4:
			a += ((uint32) k[3] << 24);
		case 3:
			a += ((uint32) k[2] << 16);
		case 2:
			a += ((uint32) k[1] << 8);
		case 1:
			a += k[0];
			/* case 0: nothing left to add */
	}
	mix_old(a, b, c);

	/* report the result */
	return c;
}


/*
 * New Postgres hashtext()
 */

#define UINT32_ALIGN_MASK 3

#define mix_new(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);  c += b; \
  b -= a;  b ^= rot(a, 6);  a += c; \
  c -= b;  c ^= rot(b, 8);  b += a; \
  a -= c;  a ^= rot(c,16);  c += b; \
  b -= a;  b ^= rot(a,19);  a += c; \
  c -= b;  c ^= rot(b, 4);  b += a; \
}

#define final_new(a,b,c) \
{ \
  c ^= b; c -= rot(b,14); \
  a ^= c; a -= rot(c,11); \
  b ^= a; b -= rot(a,25); \
  c ^= b; c -= rot(b,16); \
  a ^= c; a -= rot(c, 4); \
  b ^= a; b -= rot(a,14); \
  c ^= b; c -= rot(b,24); \
}

static uint32_t hash_new_hashtext(const void *_k, unsigned keylen)
{
	const unsigned char *k = _k;
	uint32_t a, b, c, len;

	/* Set up the internal state */
	len = keylen;
	a = b = c = 0x9e3779b9 + len + 3923095;

	/* If the source pointer is word-aligned, we use word-wide fetches */
	if (((long) k & UINT32_ALIGN_MASK) == 0)
	{
		/* Code path for aligned source data */
		register const uint32_t *ka = (const uint32_t *) k;

		/* handle most of the key */
		while (len >= 12)
		{
			a += ka[0];
			b += ka[1];
			c += ka[2];
			mix_new(a, b, c);
			ka += 3;
			len -= 12;
		}

		/* handle the last 11 bytes */
		k = (const unsigned char *) ka;
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 8);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 24);
				/* the lowest byte of c is reserved for the length */
				/* fall through */
			case 8:
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 8);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 16);
				/* fall through */
			case 5:
				b += ((uint32) k[4] << 24);
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 8);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 16);
				/* fall through */
			case 1:
				a += ((uint32) k[0] << 24);
			/* case 0: nothing left to add */
		}
#else /* !WORDS_BIGENDIAN */
		switch (len)
		{
			case 11:
				c += ((uint32) k[10] << 24);
				/* fall through */
			case 10:
				c += ((uint32) k[9] << 16);
				/* fall through */
			case 9:
				c += ((uint32) k[8] << 8);
				/* the lowest byte of c is reserved for the length */
				/* fall through */
			case 8:
				b += ka[1];
				a += ka[0];
				break;
			case 7:
				b += ((uint32) k[6] << 16);
				/* fall through */
			case 6:
				b += ((uint32) k[5] << 8);
				/* fall through */
			case 5:
				b += k[4];
				/* fall through */
			case 4:
				a += ka[0];
				break;
			case 3:
				a += ((uint32) k[2] << 16);
				/* fall through */
			case 2:
				a += ((uint32) k[1] << 8);
				/* fall through */
			case 1:
				a += k[0];
			/* case 0: nothing left to add */
		}
#endif /* WORDS_BIGENDIAN */
	}
	else
	{
		/* Code path for non-aligned source data */

		/* handle most of the key */
		while (len >= 12)
		{
#ifdef WORDS_BIGENDIAN
			a += (k[3] + ((uint32) k[2] << 8) + ((uint32) k[1] << 16) + ((uint32) k[0] << 24));
			b += (k[7] + ((uint32) k[6] << 8) + ((uint32) k[5] << 16) + ((uint32) k[4] << 24));
			c += (k[11] + ((uint32) k[10] << 8) + ((uint32) k[9] << 16) + ((uint32) k[8] << 24));
#else /* !WORDS_BIGENDIAN */
			a += (k[0] + ((uint32) k[1] << 8) + ((uint32) k[2] << 16) + ((uint32) k[3] << 24));
			b += (k[4] + ((uint32) k[5] << 8) + ((uint32) k[6] << 16) + ((uint32) k[7] << 24));
			c += (k[8] + ((uint32) k[9] << 8) + ((uint32) k[10] << 16) + ((uint32) k[11] << 24));
#endif /* WORDS_BIGENDIAN */
			mix_new(a, b, c);
			k += 12;
			len -= 12;
		}

		/* handle the last 11 bytes */
#ifdef WORDS_BIGENDIAN
		switch (len)			/* all the case statements fall through */
		{
			case 11:
				c += ((uint32) k[10] << 8);
			case 10:
				c += ((uint32) k[9] << 16);
			case 9:
				c += ((uint32) k[8] << 24);
				/* the lowest byte of c is reserved for the length */
			case 8:
				b += k[7];
			case 7:
				b += ((uint32) k[6] << 8);
			case 6:
				b += ((uint32) k[5] << 16);
			case 5:
				b += ((uint32) k[4] << 24);
			case 4:
				a += k[3];
			case 3:
				a += ((uint32) k[2] << 8);
			case 2:
				a += ((uint32) k[1] << 16);
			case 1:
				a += ((uint32) k[0] << 24);
			/* case 0: nothing left to add */
		}
#else /* !WORDS_BIGENDIAN */
		switch (len)			/* all the case statements fall through */
		{
			case 11:
				c += ((uint32) k[10] << 24);
			case 10:
				c += ((uint32) k[9] << 16);
			case 9:
				c += ((uint32) k[8] << 8);
				/* the lowest byte of c is reserved for the length */
			case 8:
				b += ((uint32) k[7] << 24);
			case 7:
				b += ((uint32) k[6] << 16);
			case 6:
				b += ((uint32) k[5] << 8);
			case 5:
				b += k[4];
			case 4:
				a += ((uint32) k[3] << 24);
			case 3:
				a += ((uint32) k[2] << 16);
			case 2:
				a += ((uint32) k[1] << 8);
			case 1:
				a += k[0];
			/* case 0: nothing left to add */
		}
#endif /* WORDS_BIGENDIAN */
	}

	final_new(a, b, c);

	/* report the result */
	return c;
}

/*
 * Get string data from Python object.
 */

static Py_ssize_t get_buffer(PyObject *obj, unsigned char **buf_p, PyObject **tmp_obj_p)
{
	PyBufferProcs *bfp;
	PyObject *str = NULL;
	Py_ssize_t res;

	/* check for None */
	if (obj == Py_None) {
		PyErr_Format(PyExc_TypeError, "None is not allowed");
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

static PyObject *run_hash(PyObject *args, hash_fn_t real_hash)
{
	unsigned char *src = NULL;
        Py_ssize_t src_len;
	PyObject *arg, *strtmp = NULL;
	int32_t hash;

        if (!PyArg_ParseTuple(args, "O", &arg))
                return NULL;
	src_len = get_buffer(arg, &src, &strtmp);
	if (src_len < 0)
		return NULL;
	hash = real_hash(src, src_len);
	Py_CLEAR(strtmp);
	return PyInt_FromLong(hash);
}

/*
 * Python wrappers around actual hash functions.
 */

static PyObject *hashtext_old(PyObject *self, PyObject *args)
{
	return run_hash(args, hash_old_hashtext);
}

static PyObject *hashtext_new(PyObject *self, PyObject *args)
{
	return run_hash(args, hash_new_hashtext);
}

/*
 * Module initialization
 */

static PyMethodDef methods[] = {
	{ "hashtext_old", hashtext_old, METH_VARARGS, "Old Postgres hashtext().\n" },
	{ "hashtext_new", hashtext_new, METH_VARARGS, "New Postgres hashtext().\n" },
	{ NULL }
};

PyMODINIT_FUNC
init_chashtext(void)
{
	PyObject *module;
	module = Py_InitModule("_chashtext", methods);
	PyModule_AddStringConstant(module, "__doc__", "String hash functions");
}

