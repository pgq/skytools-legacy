"""File utilities

>>> import tempfile, os
>>> pidfn = tempfile.mktemp('.pid')
>>> write_atomic(pidfn, "1")
>>> write_atomic(pidfn, "2")
>>> os.remove(pidfn)
>>> write_atomic(pidfn, "1", '.bak')
>>> write_atomic(pidfn, "2", '.bak')
>>> os.remove(pidfn)
"""

import sys
import os
import errno

__all__ = ['write_atomic', 'signal_pidfile']

# non-win32
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

    # win32 does not like replace
    if sys.platform == 'win32':
        try:
            os.remove(fn)
        except:
            pass

    # atomically replace file
    os.rename(fn2, fn)

def signal_pidfile(pidfile, sig):
    """Send a signal to process whose ID is located in pidfile.

    Read only first line of pidfile to support multiline
    pidfiles like postmaster.pid.

    Returns True is successful, False if pidfile does not exist
    or process itself is dead.  Any other errors will passed
    as exceptions."""

    ln = ''
    try:
        f = open(pidfile, 'r')
        ln = f.readline().strip()
        f.close()
        pid = int(ln)
        if sig == 0 and sys.platform == 'win32':
            return win32_detect_pid(pid)
        os.kill(pid, sig)
        return True
    except IOError, ex:
        if ex.errno != errno.ENOENT:
            raise
    except OSError, ex:
        if ex.errno != errno.ESRCH:
            raise
    except ValueError, ex:
        # this leaves slight race when someone is just creating the file,
        # but more common case is old empty file.
        if not ln:
            return False
        raise ValueError('Corrupt pidfile: %s' % pidfile)
    return False

def win32_detect_pid(pid):
    """Process detection for win32."""

    # avoid pywin32 dependecy, use ctypes instead
    import ctypes

    # win32 constants
    PROCESS_QUERY_INFORMATION = 1024
    STILL_ACTIVE = 259
    ERROR_INVALID_PARAMETER = 87
    ERROR_ACCESS_DENIED = 5

    # Load kernel32.dll
    k = ctypes.windll.kernel32
    OpenProcess = k.OpenProcess
    OpenProcess.restype = ctypes.c_void_p

    # query pid exit code
    h = OpenProcess(PROCESS_QUERY_INFORMATION, 0, pid)
    if h == None:
        err = k.GetLastError()
        if err == ERROR_INVALID_PARAMETER:
            return False
        if err == ERROR_ACCESS_DENIED:
            return True
        raise OSError(errno.EFAULT, "Unknown win32error: " + str(err))
    code = ctypes.c_int()
    k.GetExitCodeProcess(h, ctypes.byref(code))
    k.CloseHandle(h)
    return code.value == STILL_ACTIVE

def win32_write_atomic(fn, data, bakext=None, mode='b'):
    """Write file with rename for win32."""

    if mode not in ['', 'b', 't']:
        raise ValueError("unsupported fopen mode")

    # write new data to tmp file
    fn2 = fn + '.new'
    f = open(fn2, 'w' + mode)
    f.write(data)
    f.close()

    # move old data to bak file
    if bakext:
        if bakext.find('/') >= 0:
            raise ValueError("invalid bakext")
        fnb = fn + bakext
        try:
            os.remove(fnb)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise
        try:
            os.rename(fn, fnb)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise
    else:
        try:
            os.remove(fn)
        except:
            pass

    # replace file
    os.rename(fn2, fn)

if sys.platform == 'win32':
    write_atomic = win32_write_atomic

if __name__ == '__main__':
    import doctest
    doctest.testmod()

