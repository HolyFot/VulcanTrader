"""Lazily loads ccxt's per-exchange classes on first access instead of at import time.

ccxt's ``ccxt``, ``ccxt.async_support`` and ``ccxt.pro`` packages each
unconditionally import all ~107 supported exchanges in their ``__init__.py``
(``from ccxt.<name> import <name>`` for every single one), even though a
VulcanTrader bot process only ever talks to whichever single exchange is
configured. Measured cost: ~105MB RSS across the three facades combined - more
than pandas+numpy together - almost all of it dead weight for the ~106
exchanges a given process never touches.

This installs a ``sys.meta_path`` finder that intercepts only those three
top-level package modules, strips their eager per-exchange import statements
out of the parsed AST before executing it (everything else - the Exchange base
class, error hierarchy, the ``exchanges`` name list, precision helpers - stays
untouched and eager, since it's cheap and other code depends on it being
present immediately), and replaces the missing names with a PEP 562 module
``__getattr__`` that imports a given exchange's real module normally (and
caches the result as a plain attribute) the first time anything actually asks
for it - e.g. ``ccxt.hyperliquid``, or ``getattr(ccxt.pro, exchange_name)``.

Because this triggers on ordinary attribute access rather than a fixed
config-driven allowlist, it stays fully dynamic for the life of the process:
an exchange nobody has asked for yet is never loaded, and one requested later
(e.g. another connected client asking to trade a different exchange mid-run)
loads correctly on that first ask, with no restart required.

Must be installed before anything does ``import ccxt`` / ``import
ccxt.async_support`` / ``import ccxt.pro`` anywhere in the process. Called at
the very top of ``VulcanTrader/util/__init__.py`` (``VulcanTrader.util`` is the
lowest-level package - ``VulcanTrader.util.ft_precise`` itself does ``from
ccxt import Precise``, so this needs to run before even that) and, for
defense-in-depth, again at the top of ``VulcanTrader/exchange/__init__.py``.
``install()`` is idempotent, so calling it from both costs nothing.
"""

from __future__ import annotations

import ast
import functools
import importlib
import importlib.abc
import importlib.machinery
import logging
import sys
import threading
from types import ModuleType


logger = logging.getLogger(__name__)

_PATCHED_PACKAGES = ("ccxt", "ccxt.async_support", "ccxt.pro")

_installed = False

# Guards the import+cache sequence in _lazy_exchange_getattr. Needed because
# importlib.import_module("ccxt.<name>") has a CPython side effect outside our
# control: finishing a submodule import always binds it onto the parent
# package (`ccxt.<name> = <submodule object>`), and ccxt's own convention is
# that the submodule and the class inside it share the same name (the module
# `ccxt.binance` defines a class also called `binance`). That leaves a real,
# if narrow, window where `ccxt.<name>` briefly holds the submodule rather
# than the class, between Python's own auto-bind and our setattr() below - if
# a second thread reads the attribute directly (not through __getattr__,
# since the attribute now technically exists) in that window, it gets the
# module and a `TypeError: 'module' object is not callable` on the next
# line. A single lock around the whole sequence collapses concurrent
# first-time accesses to one importer at a time, which is what actually
# closes the window in practice (confirmed via a 32-thread stress test that
# reproduced the race without the lock and didn't with it) - must be an RLock,
# not a plain Lock: ccxt.pro exchange classes are DEFINED as subclasses of
# their ccxt.async_support counterpart (e.g. `class hyperliquid(ccxt.
# async_support.hyperliquid):` in ccxt/pro/hyperliquid.py), so importing
# ccxt.pro.hyperliquid recurses straight back into this same function, on the
# same thread, to fetch ccxt.async_support.hyperliquid before the outer call
# has returned. A plain Lock would self-deadlock on that re-entry (confirmed -
# hung indefinitely until switched to RLock).
# reproduced the race without the lock and didn't with it).
_load_lock = threading.RLock()


def _is_eager_exchange_import(node: ast.stmt, package_prefix: str) -> bool:
    """True for a top-level ``from <package_prefix>.<name> import <name>`` node.

    That's ccxt's per-exchange import pattern. Deliberately excludes anything
    with extra dots after the prefix (``<package_prefix>.base.exchange`` etc.)
    so the real base-class/error/precision imports are left alone untouched.
    """
    if not isinstance(node, ast.ImportFrom) or not node.module:
        return False
    prefix = package_prefix + "."
    if not node.module.startswith(prefix):
        return False
    suffix = node.module[len(prefix) :]
    if "." in suffix or suffix == "base":
        return False
    names = {alias.name for alias in node.names}
    return names == {suffix}


def _lazy_exchange_getattr(module: ModuleType, package_prefix: str, name: str):
    known = getattr(module, "exchanges", ())
    if name not in known:
        raise AttributeError(f"module {module.__name__!r} has no attribute {name!r}")
    with _load_lock:
        # Re-check: another thread may have already imported *and* fixed up
        # this name while we were waiting for the lock (or Python's own
        # parent-package auto-bind briefly left the raw submodule sitting
        # under this name - overwrite it with the class either way).
        cls = module.__dict__.get(name)
        if not isinstance(cls, type):
            submodule = importlib.import_module(f"{package_prefix}.{name}")
            cls = getattr(submodule, name)
            setattr(module, name, cls)  # cache: next access hits __dict__ directly
            logger.debug("Lazily loaded ccxt exchange %s.%s", package_prefix, name)
    return cls


class _LazyCcxtLoader(importlib.abc.Loader):
    """Wraps ccxt's real loader, executing its ``__init__.py`` with the eager
    per-exchange imports stripped out of the parsed source before exec."""

    def __init__(self, orig_loader, package_prefix: str):
        self._orig_loader = orig_loader
        self._package_prefix = package_prefix

    def create_module(self, spec):
        return None  # defer to default module creation (sets __path__ etc. from spec)

    def exec_module(self, module: ModuleType) -> None:
        fullname = module.__name__
        source = self._orig_loader.get_source(fullname)
        filename = self._orig_loader.get_filename(fullname)
        tree = ast.parse(source, filename=filename)
        stripped = 0
        new_body = []
        for node in tree.body:
            if _is_eager_exchange_import(node, self._package_prefix):
                stripped += 1
                continue
            new_body.append(node)
        tree.body = new_body
        ast.fix_missing_locations(tree)
        code = compile(tree, filename=filename, mode="exec")
        exec(code, module.__dict__)
        module.__getattr__ = functools.partial(_lazy_exchange_getattr, module, self._package_prefix)
        logger.debug(
            "Patched %s: deferred %d eager exchange imports to lazy on-demand loading",
            fullname,
            stripped,
        )


class _LazyCcxtFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname not in _PATCHED_PACKAGES:
            return None
        # Delegate straight to the filesystem finder (not sys.meta_path) to avoid recursing
        # back into this same finder.
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec is None or spec.loader is None:
            return None
        spec.loader = _LazyCcxtLoader(spec.loader, fullname)
        return spec


def install() -> None:
    """Install the lazy-loading meta path finder. Idempotent and safe to call
    from multiple entry points - only the first call does anything."""
    global _installed
    if _installed:
        return
    already_loaded = [name for name in _PATCHED_PACKAGES if name in sys.modules]
    if already_loaded:
        logger.warning(
            "ccxt lazy-loader installed too late: %s already imported eagerly; "
            "their exchange classes are already resident and won't be trimmed.",
            ", ".join(already_loaded),
        )
    sys.meta_path.insert(0, _LazyCcxtFinder())
    _installed = True
