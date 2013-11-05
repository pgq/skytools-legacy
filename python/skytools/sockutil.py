"""Various low-level utility functions for sockets."""

__all__ = ['set_tcp_keepalive', 'set_nonblocking', 'set_cloexec']

import sys
import os
import socket

try:
    import fcntl
except ImportError:
    pass

__all__ = ['set_tcp_keepalive', 'set_nonblocking', 'set_cloexec']

def set_tcp_keepalive(fd, keepalive = True,
                     tcp_keepidle = 4 * 60,
                     tcp_keepcnt = 4,
                     tcp_keepintvl = 15):
    """Turn on TCP keepalive.  The fd can be either numeric or socket
    object with 'fileno' method.

    OS defaults for SO_KEEPALIVE=1:
     - Linux: (7200, 9, 75) - can configure all.
     - MacOS: (7200, 8, 75) - can configure only tcp_keepidle.
     - Win32: (7200, 5|10, 1) - can configure tcp_keepidle and tcp_keepintvl.
       Python needs SIO_KEEPALIVE_VALS support in socket.ioctl to enable it.

    Our defaults: (240, 4, 15).

    >>> import socket
    >>> s = socket.socket()
    >>> set_tcp_keepalive(s)
    """

    # usable on this OS?
    if not hasattr(socket, 'SO_KEEPALIVE') or not hasattr(socket, 'fromfd'):
        return

    # need socket object
    if isinstance(fd, socket.SocketType):
        s = fd
    else:
        if hasattr(fd, 'fileno'):
            fd = fd.fileno()
        s = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)

    # skip if unix socket
    if type(s.getsockname()) != type(()):
        return

    # turn on keepalive on the connection
    if keepalive:
        DARWIN_TCP_KEEPALIVE = 0x10

        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, 'TCP_KEEPCNT'):
            s.setsockopt(socket.IPPROTO_TCP, getattr(socket, 'TCP_KEEPCNT'), tcp_keepcnt)
            if hasattr(socket, 'TCP_KEEPINTVL'):
                s.setsockopt(socket.IPPROTO_TCP, getattr(socket, 'TCP_KEEPINTVL'), tcp_keepintvl)
            if hasattr(socket, 'TCP_KEEPIDLE'):
                s.setsockopt(socket.IPPROTO_TCP, getattr(socket, 'TCP_KEEPIDLE'), tcp_keepidle)
            elif sys.platform == 'darwin':
                s.setsockopt(socket.IPPROTO_TCP, DARWIN_TCP_KEEPALIVE, tcp_keepidle)
        elif hasattr(socket, 'TCP_KEEPALIVE'):
            s.setsockopt(socket.IPPROTO_TCP, getattr(socket, 'TCP_KEEPALIVE'), tcp_keepidle)
        elif sys.platform == 'darwin':
            s.setsockopt(socket.IPPROTO_TCP, DARWIN_TCP_KEEPALIVE, tcp_keepidle)
        elif sys.platform == 'win32':
            #s.ioctl(SIO_KEEPALIVE_VALS, (1, tcp_keepidle*1000, tcp_keepintvl*1000))
            pass
    else:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0)


def set_nonblocking(fd, onoff=True):
    """Toggle the O_NONBLOCK flag.

    If onoff==None then return current setting.

    Actual sockets from 'socket' module should use .setblocking() method,
    this is for situations where it is not available.  Eg. pipes
    from 'subprocess' module.

    >>> import socket
    >>> s = socket.socket()
    >>> set_nonblocking(s, None)
    False
    >>> set_nonblocking(s, 1)
    >>> set_nonblocking(s, None)
    True
    """

    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    if onoff is None:
        return (flags & os.O_NONBLOCK) > 0
    if onoff:
        flags |= os.O_NONBLOCK
    else:
        flags &= ~os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, flags)

def set_cloexec(fd, onoff=True):
    """Toggle the FD_CLOEXEC flag.

    If onoff==None then return current setting.

    Some libraries do it automatically (eg. libpq).
    Others do not (Python stdlib).

    >>> import os
    >>> f = open(os.devnull, 'rb')
    >>> set_cloexec(f, None)
    False
    >>> set_cloexec(f, True)
    >>> set_cloexec(f, None)
    True
    >>> import socket
    >>> s = socket.socket()
    >>> set_cloexec(s, None)
    False
    >>> set_cloexec(s)
    >>> set_cloexec(s, None)
    True
    """

    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    if onoff is None:
        return (flags & fcntl.FD_CLOEXEC) > 0
    if onoff:
        flags |= fcntl.FD_CLOEXEC
    else:
        flags &= ~fcntl.FD_CLOEXEC
    fcntl.fcntl(fd, fcntl.F_SETFD, flags)

if __name__ == '__main__':
    import doctest
    doctest.testmod()

