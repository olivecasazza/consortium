"""Fallback: re-export the original pure-Python ClusterShell modules.

This sub-package is a symlink/copy bridge so that any symbol not yet
ported to Rust can be imported from `ClusterShell._py.ModuleName`.
The actual source lives in the repo root at `lib/ClusterShell/`.
"""

import sys
import os

# Add the original lib/ directory to sys.path so imports resolve.
_lib_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "lib"))
if _lib_dir not in sys.path:
    sys.path.insert(0, _lib_dir)

# Re-export everything from the original package under the _py namespace.
# Each wrapper module (e.g. ClusterShell/RangeSet.py) imports from
# ClusterShell._py.RangeSet which triggers the original code.
import importlib as _importlib

def __getattr__(name):
    """Lazily import original ClusterShell submodules."""
    try:
        mod = _importlib.import_module(f"ClusterShell.{name}", package=None)
        return mod
    except ImportError:
        raise AttributeError(f"module 'ClusterShell._py' has no attribute {name!r}")
