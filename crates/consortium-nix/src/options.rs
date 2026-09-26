//! Per-run deployment settings beyond the positional arguments of
//! [`crate::deploy`]: extra nix words per platform and hosts to activate
//! locally.

use crate::config::ProfileType;

/// Extra words appended to every `nix eval` / `nix build` for a platform,
/// e.g. `--override-input foo path:./stub` to keep a NixOS host's closure
/// from pulling an input only the darwin hosts need.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NixArgs {
    /// Appended for `darwinConfigurations.*` evaluations and builds.
    pub darwin: Vec<String>,
    /// Appended for `nixosConfigurations.*` evaluations and builds.
    pub nixos: Vec<String>,
}

impl NixArgs {
    /// The words for one profile type.
    pub fn for_profile(&self, profile_type: &ProfileType) -> &[String] {
        match profile_type {
            ProfileType::Nixos => &self.nixos,
            ProfileType::NixDarwin => &self.darwin,
        }
    }
}

/// Options for [`crate::deploy_with_options`] and
/// [`crate::deploy_with_cascade_options`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct DeployOptions {
    /// Per-platform extra nix arguments.
    pub nix_args: NixArgs,
    /// Node names that are *this* machine: their closure is not copied and
    /// activation runs locally through `sudo` instead of over ssh.
    pub local_hosts: Vec<String>,
}

impl DeployOptions {
    /// No extra nix arguments, no local hosts.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the per-platform nix arguments.
    pub fn nix_args(mut self, nix_args: NixArgs) -> Self {
        self.nix_args = nix_args;
        self
    }

    /// Set the node names activated locally.
    pub fn local_hosts<I, S>(mut self, hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.local_hosts = hosts.into_iter().map(Into::into).collect();
        self
    }

    /// Whether `host` is activated locally.
    pub fn is_local(&self, host: &str) -> bool {
        self.local_hosts.iter().any(|h| h == host)
    }
}
