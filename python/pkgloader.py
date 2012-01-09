"""Loader for Skytools modules.

Primary idea is to allow several major versions to co-exists.
Secondary idea - allow checking minimal minor version.

"""

import sys, os, os.path, re

__all__ = ['require']

_top = os.path.dirname(os.path.abspath(os.path.normpath(__file__)))

_pkg_cache = None
_import_cache = {}
_pat = re.compile('^([a-z]+)-([0-9]+).([0-9]+)$')

def _load_pkg_cache():
    global _pkg_cache
    if _pkg_cache is not None:
        return _pkg_cache
    _pkg_cache = {}
    for dir in os.listdir(_top):
        m = _pat.match(dir)
        if not m:
            continue
        modname = m.group(1)
        modver = (int(m.group(2)), int(m.group(3)))
        _pkg_cache.setdefault(modname, []).append((modver, dir))
    for vlist in _pkg_cache.itervalues():
        vlist.sort(reverse = True)
    return _pkg_cache

def _install_path(pkg, newpath):
    for p in sys.path:
        pname = os.path.basename(p)
        m = _pat.match(pname)
        if m and m.group(1) == pkg:
            sys.path.remove(p)
    sys.path.insert(0, newpath)

def require(pkg, reqver):
    # parse arg
    reqval = tuple([int(n) for n in reqver.split('.')])
    need = reqval[:2] # cut minor ver

    # check if we already have one installed
    if pkg in _import_cache:
        got = _import_cache[pkg]
        if need[0] != got[0] or reqval > got:
            raise ImportError("Request for package '%s' ver '%s', have '%s'" % (
                              pkg, reqver, '.'.join(got)))
        return

    # pick best ver from available ones
    cache = _load_pkg_cache()
    if pkg not in cache:
        return

    for pkgver, pkgdir in cache[pkg]:
        if pkgver[0] == need[0] and pkgver >= need:
            # install the best on
            _install_path(pkg, os.path.join(_top, pkgdir))
            break

    inst_ver = reqval

    # now import whatever is available
    mod = __import__(pkg)

    # check if it is actually useful
    ver_str = mod.__version__
    for i, c in enumerate(ver_str):
        if c != '.' and not c.isdigit():
            ver_str = ver_str[:i]
            break
    full_ver = tuple([int(x) for x in ver_str.split('.')])
    if full_ver[0] != reqval[0] or reqval > full_ver:
        raise ImportError("Request for package '%s' ver '%s', have '%s'" % (
                          pkg, reqver, '.'.join(full_ver)))
    inst_ver = full_ver

    # remember full version
    _import_cache[pkg] = inst_ver

    return mod

