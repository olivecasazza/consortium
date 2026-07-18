//! Configuration types for NixOS deployment.
//!
//! These types map to the JSON produced by the Nix library's `mkFleet` function.
//!
//! The definitions live in [`consortium_integration::fleet`] so that all
//! integration crates can share them without depending on the nix deployment
//! pipeline; this module re-exports them wholesale so existing
//! `crate::config::*` and `consortium_nix::config::*` imports keep working.

pub use consortium_integration::fleet::*;
