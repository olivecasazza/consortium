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

if BACKEND == "python":
    # Redirect all ClusterShell imports to the original pure-Python source.
    # We do this by inserting lib/ at the front of sys.path and removing
    # this package's directory so Python resolves to the original.
    _this_dir = os.path.dirname(os.path.abspath(__file__))
    _lib_dir = os.path.normpath(os.path.join(_this_dir, "..", "..", "..", "..", "lib"))

    if not os.path.isdir(_lib_dir):
        # CI may set LIB_CLUSTERSHELL to point to the upstream lib/
        _lib_dir = os.environ.get("LIB_CLUSTERSHELL", _lib_dir)

    if os.path.isdir(_lib_dir):
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
            f"CONSORTIUM_BACKEND=python but cannot find original ClusterShell at {_lib_dir}. "
            f"Set LIB_CLUSTERSHELL env var to the lib/ directory containing the original."
        )
