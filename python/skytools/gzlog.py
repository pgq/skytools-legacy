
"""Atomic append of gzipped data.

The point is - if several gzip streams are concatenated,
they are read back as one whole stream.
"""

import gzip
from cStringIO import StringIO

__all__ = ['gzip_append']

#
# gzip storage
#
def gzip_append(filename, data, level = 6):
    """Append a block of data to file with safety checks."""

    # compress data
    buf = StringIO()
    g = gzip.GzipFile(fileobj = buf, compresslevel = level, mode = "w")
    g.write(data)
    g.close()
    zdata = buf.getvalue()

    # append, safely
    f = open(filename, "a+", 0)
    f.seek(0, 2)
    pos = f.tell()
    try:
        f.write(zdata)
        f.close()
    except Exception, ex:
        # rollback on error
        f.seek(pos, 0)
        f.truncate()
        f.close()
        raise ex
