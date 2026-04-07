"""ClusterShell.RangeSet — Rust-backed via consortium."""

from ClusterShell._consortium import RangeSet

# Re-export error types
from ClusterShell._consortium import RangeSetParseError

# Constants
AUTOSTEP_DISABLED = 0xFFFFFFFF
