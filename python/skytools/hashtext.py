"""
Implementation of Postgres hashing function.

hashtext_old() - used up to PostgreSQL 8.3
hashtext_new() - used since PostgreSQL 8.4

>>> import skytools._chashtext
>>> for i in range(3):
...     print [hashtext_new_py('x' * (i*5 + j)) for j in range(5)]
[-1477818771, 1074944137, -1086392228, -1992236649, -1379736791]
[-370454118, 1489915569, -66683019, -2126973000, 1651296771]
[755764456, -1494243903, 631527812, 28686851, -9498641]
>>> for i in range(3):
...     print [hashtext_old_py('x' * (i*5 + j)) for j in range(5)]
[-863449762, 37835117, 294739542, -320432768, 1007638138]
[1422906842, -261065348, 59863994, -162804943, 1736144510]
[-682756517, 317827663, -495599455, -1411793989, 1739997714]
>>> data = 'HypficUjFitraxlumCitcemkiOkIkthi'
>>> p = [hashtext_old_py(data[:l]) for l in range(len(data)+1)]
>>> c = [hashtext_old(data[:l]) for l in range(len(data)+1)]
>>> assert p == c, '%s <> %s' % (p, c)
>>> p == c
True
>>> p = [hashtext_new_py(data[:l]) for l in range(len(data)+1)]
>>> c = [hashtext_new(data[:l]) for l in range(len(data)+1)]
>>> assert p == c, '%s <> %s' % (p, c)
>>> p == c
True
"""

import sys, struct

__all__ = ["hashtext_old", "hashtext_new"]

# pad for last partial block
PADDING = '\0' * 12

def uint32(x):
    """python does not have 32 bit integer so we need this hack to produce uint32 after bit operations"""
    return x & 0xffffffff

#
# Old Postgres hashtext() - lookup2 with custom initval
#

FMT_OLD = struct.Struct("<LLL")

def mix_old(a,b,c):
    c = uint32(c)

    a -= b; a -= c; a = uint32(a ^ (c>>13))
    b -= c; b -= a; b = uint32(b ^ (a<<8))
    c -= a; c -= b; c = uint32(c ^ (b>>13))
    a -= b; a -= c; a = uint32(a ^ (c>>12))
    b -= c; b -= a; b = uint32(b ^ (a<<16))
    c -= a; c -= b; c = uint32(c ^ (b>>5))
    a -= b; a -= c; a = uint32(a ^ (c>>3))
    b -= c; b -= a; b = uint32(b ^ (a<<10))
    c -= a; c -= b; c = uint32(c ^ (b>>15))

    return a, b, c

def hashtext_old_py(k):
    """Old Postgres hashtext()"""

    remain = len(k)
    pos = 0
    a = b = 0x9e3779b9
    c = 3923095

    # handle most of the key
    while remain >= 12:
        a2, b2, c2 = FMT_OLD.unpack_from(k, pos)
        a, b, c = mix_old(a + a2, b + b2, c + c2)
        pos += 12;
        remain -= 12;

    # handle the last 11 bytes
    a2, b2, c2 = FMT_OLD.unpack_from(k[pos:] + PADDING, 0)

    # the lowest byte of c is reserved for the length
    c2 = (c2 << 8) + len(k)

    a, b, c = mix_old(a + a2, b + b2, c + c2)

    # convert to signed int
    if (c & 0x80000000):
        c = -0x100000000 + c

    return int(c)

#
# New Postgres hashtext() - hacked lookup3:
# - custom initval
# - calls mix() when len=12
# - shifted c in last block on little-endian
#

FMT_NEW = struct.Struct("=LLL")

def rol32(x,k):
    return (((x)<<(k)) | (uint32(x)>>(32-(k))))

def mix_new(a,b,c):
    a -= c;  a ^= rol32(c, 4);  c += b
    b -= a;  b ^= rol32(a, 6);  a += c
    c -= b;  c ^= rol32(b, 8);  b += a
    a -= c;  a ^= rol32(c,16);  c += b
    b -= a;  b ^= rol32(a,19);  a += c
    c -= b;  c ^= rol32(b, 4);  b += a

    return uint32(a), uint32(b), uint32(c)

def final_new(a,b,c):
    c ^= b; c -= rol32(b,14)
    a ^= c; a -= rol32(c,11)
    b ^= a; b -= rol32(a,25)
    c ^= b; c -= rol32(b,16)
    a ^= c; a -= rol32(c, 4)
    b ^= a; b -= rol32(a,14)
    c ^= b; c -= rol32(b,24)

    return uint32(a), uint32(b), uint32(c)

def hashtext_new_py(k):
    """New Postgres hashtext()"""
    remain = len(k)
    pos = 0
    a = b = c = 0x9e3779b9 + len(k) + 3923095

    # handle most of the key
    while remain >= 12:
        a2, b2, c2 = FMT_NEW.unpack_from(k, pos)
        a, b, c = mix_new(a + a2, b + b2, c + c2)
        pos += 12;
        remain -= 12;

    # handle the last 11 bytes
    a2, b2, c2 = FMT_NEW.unpack_from(k[pos:] + PADDING, 0)
    if sys.byteorder == 'little':
        c2 = c2 << 8
    a, b, c = final_new(a + a2, b + b2, c + c2)

    # convert to signed int
    if (c & 0x80000000):
        c = -0x100000000 + c

    return int(c)


try:
    from skytools._chashtext import hashtext_old, hashtext_new
except ImportError:
    hashtext_old = hashtext_old_py
    hashtext_new = hashtext_new_py


# run doctest
if __name__ == '__main__':
    import doctest
    doctest.testmod()
