"""ClusterShell — dual-backend package for consortium migration.

Backend selection via CONSORTIUM_BACKEND environment variable:
  - "rust"   (default): Rust-backed via PyO3 (_consortium)
  - "python":           Original pure-Python from lib/ClusterShell/

When backend is "python", we rewrite sys.path so that imports resolve
to lib/ClusterShell/ (the upstream pure-Python implementation).
"""

import os
import sys

__version__ = "0.1.0"

BACKEND = os.environ.get("CONSORTIUM_BACKEND", "rust")

_this_dir = os.path.dirname(os.path.abspath(__file__))


def _find_oracle_lib():
    """Locate the lib/ directory holding the original pure-Python ClusterShell.

    Search order:
      1. LIB_CLUSTERSHELL env var (explicit override)
      2. Sibling checkout: ../consortium-tests/lib (split-repo layout — the
         oracle and parity suite live in the consortium-tests repo)
      3. In-repo lib/ (legacy monorepo layout)
    """
    candidates = []
    env_dir = os.environ.get("LIB_CLUSTERSHELL")
    if env_dir:
        candidates.append(env_dir)
    _repo_root = os.path.normpath(os.path.join(_this_dir, "..", "..", ".."))
    candidates.append(
        os.path.join(os.path.dirname(_repo_root), "consortium-tests", "lib")
    )
    candidates.append(os.path.join(_repo_root, "lib"))
    for cand in candidates:
        if os.path.isdir(cand):
            return os.path.normpath(cand)
    return None


if BACKEND == "python":
    # Redirect all ClusterShell imports to the original pure-Python source.
    # We do this by inserting lib/ at the front of sys.path and removing
    # this package's directory so Python resolves to the original.
    _lib_dir = _find_oracle_lib()

    if _lib_dir is not None:
        # Remove this package's parent from sys.path if present
        _parent = os.path.dirname(_this_dir)
        if _parent in sys.path:
            sys.path.remove(_parent)

        # Add lib/ to front
        if _lib_dir not in sys.path:
            sys.path.insert(0, _lib_dir)

        # Remove ourselves from sys.modules so the next import of
        # ClusterShell resolves to the original in lib/
        for key in list(sys.modules.keys()):
            if key == "ClusterShell" or key.startswith("ClusterShell."):
                del sys.modules[key]

        # Re-import from the original
        import importlib
        _orig = importlib.import_module("ClusterShell")
        # Copy its namespace into ours
        globals().update(_orig.__dict__)
    else:
        raise RuntimeError(
            "CONSORTIUM_BACKEND=python but cannot find the original ClusterShell "
            "lib/ (looked in $LIB_CLUSTERSHELL, ../consortium-tests/lib, and the "
            "in-repo lib/). Set LIB_CLUSTERSHELL env var to the lib/ directory "
            "containing the original."
        )
else:
    # Rust backend: let modules/subpackages that have no rust-backed shim yet
    # (CLI, Engine, Worker, Task, MsgTree, ...) resolve from the original
    # lib/ tree. Shims in this directory take precedence (they are listed
    # first in __path__), so ported modules always hit the rust backend and
    # unported ones fall back to the oracle implementation.
    _lib_dir = _find_oracle_lib()
    if _lib_dir is not None:
        _lib_pkg = os.path.join(_lib_dir, "ClusterShell")
        if os.path.isdir(_lib_pkg) and _lib_pkg not in __path__:
            __path__.append(_lib_pkg)

        # The inherited TreeWorker reduces sys.executable to its basename,
        # which loses the active environment in a remote login shell. Wrap
        # construction only for the Rust-backed package so each gateway keeps
        # the interpreter that loaded the extension. Explicit overrides remain
        # authoritative inside the native helper.
        from functools import wraps as _wraps

        from ClusterShell.Worker import Tree as _tree_worker
        from ClusterShell._consortium import _use_importing_python_for_gateway

        def _wrap_tree_worker_init(init):
            @_wraps(init)
            def rust_tree_worker_init(self, *args, **kwargs):
                init(self, *args, **kwargs)
                _use_importing_python_for_gateway(self)

            rust_tree_worker_init._consortium_gateway_wrapper = True
            return rust_tree_worker_init

        if not getattr(
            _tree_worker.TreeWorker.__init__,
            "_consortium_gateway_wrapper",
            False,
        ):
            _tree_worker.TreeWorker.__init__ = _wrap_tree_worker_init(
                _tree_worker.TreeWorker.__init__
            )
