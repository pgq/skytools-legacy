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
                              pkg, reqver, '.'.join(_skytools_required_version)))
        return

    # pick best ver from available ones
    _pkg_cache = _load_pkg_cache()
    if pkg not in _pkg_cache:
        return

    for pkgver, pkgdir in _pkg_cache[pkg]:
        if pkgver[0] == need[0] and pkgver >= need:
            # install the best on
            _install_path(pkg, os.path.join(_top, pkgdir))
            break

    # now import whatever is available
    inst_ver = reqval
    try:
        mod = __import__(pkg)
        ver_str = mod.__version__
        # check if it is actually useful
        full_ver = tuple([int(x) for x in full_str.split('.')])
        if full_ver[0] != reqval[0] or reqval > full_ver:
            raise ImportError("Request for Skytools ver '%s', got '%s'" % (
                            reqver, full_str))
            raise ImportError("Request for package '%s' ver '%s', have '%s'" % (
                              pkg, reqver, full_str))
        inst_ver = full_ver
    except ImportError:
        pass
    except AttributeError:
        pass

    # remember full version
    _import_cache[pkg] = inst_ver

    return mod


