"""ClusterShell.RangeSet — backend-aware shim.

When CONSORTIUM_BACKEND=rust (default), imports from Rust PyO3 bindings.
When CONSORTIUM_BACKEND=python, this file is never reached (the __init__.py
redirects the entire ClusterShell package to the original pure-Python source).
"""

from ClusterShell._consortium import RangeSet
from ClusterShell._consortium import RangeSetParseError

# Constants expected by upstream tests
AUTOSTEP_DISABLED = 0xFFFFFFFF
