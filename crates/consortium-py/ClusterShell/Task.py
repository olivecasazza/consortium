"""ClusterShell.Task — backend-aware shim.

When CONSORTIUM_BACKEND=rust (default), imports from Rust PyO3 bindings.
When no Rust binding exists yet, falls back to the original pure-Python source.
When CONSORTIUM_BACKEND=python, this file is never reached (the __init__.py
redirects the entire ClusterShell package to the original pure-Python source).
"""

try:
    from ClusterShell._consortium import *  # noqa: F403
except ImportError:
    # Rust binding not yet available for this module — fall back to pure Python
    import sys as _sys
    import os as _os
    _lib_dir = _os.path.normpath(
        _os.path.join(_os.path.dirname(__file__), "..", "..", "..", "..", "lib")
    )
    if _lib_dir not in _sys.path:
        _sys.path.insert(0, _lib_dir)
    from ClusterShell.Task import *  # noqa: F403
