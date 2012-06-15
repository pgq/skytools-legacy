"""File utilities"""

import os

__all__ = ['write_atomic']

def write_atomic(fn, data, bakext=None, mode='b'):
    """Write file with rename."""

    if mode not in ['', 'b', 't']:
        raise ValueError("unsupported fopen mode")

    # write new data to tmp file
    fn2 = fn + '.new'
    f = open(fn2, 'w' + mode)
    f.write(data)
    f.close()

    # link old data to bak file
    if bakext:
        if bakext.find('/') >= 0:
            raise ValueError("invalid bakext")
        fnb = fn + bakext
        try:
            os.unlink(fnb)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise
        try:
            os.link(fn, fnb)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise

    # atomically replace file
    os.rename(fn2, fn)

