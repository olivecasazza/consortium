//! Live SSH endpoint resolution.
//!
//! Fleet entries name hosts, not addresses: laptops move between DHCP
//! leases and mDNS names come and go, so the address a node had when the
//! fleet config was written is the least reliable thing about it. Before
//! copying or activating, [`EndpointResolver::resolve`] tries, in order:
//!
//! 1. the node's `targetHost` as written,
//! 2. `<name>.local` (mDNS), where `<name>` is the node name up to the
//!    first dot,
//! 3. the bare `<name>`,
//! 4. the IPv4 addresses each of the above resolves to, in the same order,
//!
//! deduplicated, and picks the first endpoint that accepts a
//! non-interactive `ssh … true`. The DNS lookup is injected so the probe
//! order is testable without touching the network.

use std::net::{IpAddr, ToSocketAddrs};

use consortium_integration::exec::{CommandSpec, Executor, SshTarget};

use crate::config::DeploymentNode;
use crate::error::{NixError, Result};

/// Default `ConnectTimeout` for endpoint probes, in seconds.
pub const DEFAULT_CONNECT_TIMEOUT: u32 = 8;

/// Name-to-address lookup used to extend the candidate list.
pub type Lookup<'a> = &'a (dyn Fn(&str) -> Vec<IpAddr> + Sync);

/// Resolve a node's live SSH endpoint by probing candidates through an
/// [`Executor`].
pub struct EndpointResolver<'a> {
    exec: &'a dyn Executor,
    lookup: Lookup<'a>,
    connect_timeout: u32,
}

impl<'a> EndpointResolver<'a> {
    /// A resolver probing through `exec` and extending candidates with
    /// `lookup` (use [`system_lookup`] in production).
    pub fn new(exec: &'a dyn Executor, lookup: Lookup<'a>) -> Self {
        Self {
            exec,
            lookup,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }

    /// Override the per-probe ssh `ConnectTimeout` (seconds).
    pub fn connect_timeout(mut self, secs: u32) -> Self {
        self.connect_timeout = secs;
        self
    }

    /// The ordered, deduplicated endpoint candidates for a node.
    pub fn candidates(&self, node: &DeploymentNode) -> Vec<String> {
        let bare = bare_name(&node.name);
        let names = [
            node.target_host.clone(),
            format!("{}.local", bare),
            bare.to_string(),
        ];
        let mut out: Vec<String> = Vec::with_capacity(names.len() * 2);
        let mut push = |c: String| {
            if !c.is_empty() && !out.contains(&c) {
                out.push(c);
            }
        };
        for n in &names {
            push(n.clone());
        }
        for n in &names {
            for ip in (self.lookup)(n) {
                if let IpAddr::V4(v4) = ip {
                    push(v4.to_string());
                }
            }
        }
        out
    }

    /// The first candidate accepting `ssh … true` as the node's user, or
    /// [`NixError::SshFailed`] listing every endpoint tried.
    pub fn resolve(&self, node: &DeploymentNode) -> Result<String> {
        let candidates = self.candidates(node);
        let timeout = format!("-oConnectTimeout={}", self.connect_timeout);
        for candidate in &candidates {
            let mut target = SshTarget::new(&node.target_user, candidate).extra_opt(&timeout);
            if let Some(port) = node.target_port {
                target = target.port(port);
            }
            let spec = CommandSpec::new("true").ssh(target);
            if matches!(self.exec.exec(&spec), Ok(out) if out.success()) {
                return Ok(candidate.clone());
            }
        }
        Err(NixError::SshFailed {
            host: node.name.clone(),
            message: format!(
                "no reachable ssh endpoint as {} (tried: {})",
                node.target_user,
                candidates.join(", ")
            ),
        })
    }
}

/// A node name up to its first dot — the flake attribute name for entries
/// written as `host.local` or `host.example.org`.
pub fn bare_name(name: &str) -> &str {
    name.split('.').next().unwrap_or(name)
}

/// This machine's short hostname (`hostname`, up to the first dot), via
/// `exec`; `None` when the command is unavailable or prints nothing.
pub fn local_hostname(exec: &dyn Executor) -> Option<String> {
    let output = exec.exec(&CommandSpec::new("hostname")).ok()?;
    if !output.success() {
        return None;
    }
    let name = bare_name(output.stdout.trim());
    (!name.is_empty()).then(|| name.to_string())
}

/// Whether node `name` is `local_hostname` — compared on bare names,
/// case-insensitively (macOS hostnames are case-preserving, not
/// case-sensitive).
pub fn is_local_node(name: &str, local_hostname: &str) -> bool {
    bare_name(name).eq_ignore_ascii_case(bare_name(local_hostname))
}

/// Resolve `name` through the system resolver (DNS, mDNS, `/etc/hosts`).
/// Failures resolve to no addresses.
pub fn system_lookup(name: &str) -> Vec<IpAddr> {
    (name, 22u16)
        .to_socket_addrs()
        .map(|addrs| addrs.map(|a| a.ip()).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProfileType;
    use consortium_integration::exec::{ExecOutput, ScriptedExecutor};
    use std::net::Ipv4Addr;

    fn node(name: &str, target_host: &str) -> DeploymentNode {
        DeploymentNode {
            name: name.into(),
            target_host: target_host.into(),
            target_user: "admin".into(),
            target_port: None,
            system: "aarch64-darwin".into(),
            profile_type: ProfileType::NixDarwin,
            build_on_target: false,
            tags: vec![],
            drv_path: None,
            toplevel: None,
        }
    }

    fn lookup(name: &str) -> Vec<IpAddr> {
        match name {
            "mac01.local" => vec![
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
                // IPv6 is skipped: `ssh-ng://user@host` store URIs take v4.
                IpAddr::V6("fe80::1".parse().unwrap()),
            ],
            "mac01" => vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))],
            _ => vec![],
        }
    }

    #[test]
    fn candidates_follow_target_local_bare_then_ipv4_deduped() {
        let exec = ScriptedExecutor::new();
        let r = EndpointResolver::new(&exec, &lookup);
        assert_eq!(
            r.candidates(&node("mac01.example.org", "mac01.example.org")),
            vec!["mac01.example.org", "mac01.local", "mac01", "10.0.0.5"]
        );
        // targetHost equal to the bare name collapses the duplicate.
        assert_eq!(
            r.candidates(&node("mac01", "mac01")),
            vec!["mac01", "mac01.local", "10.0.0.5"]
        );
    }

    #[test]
    fn resolve_returns_first_candidate_accepting_ssh() {
        // targetHost refuses (stale lease), mDNS name times out, bare
        // name answers.
        let exec = ScriptedExecutor::new()
            .on_error(
                "192.0.2.9",
                "ssh: connect to host 192.0.2.9: No route to host",
            )
            .on(
                "mac01.local",
                ExecOutput::new(255, "", "Connection timed out"),
            )
            .on(" mac01 'true'", ExecOutput::ok(""));
        let r = EndpointResolver::new(&exec, &lookup).connect_timeout(3);
        let endpoint = r.resolve(&node("mac01", "192.0.2.9")).unwrap();
        assert_eq!(endpoint, "mac01");

        let calls = exec.invocations();
        assert_eq!(calls.len(), 3, "{calls:?}");
        assert!(calls[0].contains("-oConnectTimeout=3 192.0.2.9 'true'"));
        assert!(calls[1].contains("mac01.local 'true'"));
        assert!(calls[2].contains("-l admin -oConnectTimeout=3 mac01 'true'"));
        exec.assert_invoked_containing("-oBatchMode=yes");
    }

    #[test]
    fn resolve_failure_lists_every_endpoint_tried() {
        let exec = ScriptedExecutor::new().on("", ExecOutput::new(255, "", "refused"));
        let r = EndpointResolver::new(&exec, &lookup);
        let err = r.resolve(&node("mac01", "192.0.2.9")).unwrap_err();
        match err {
            NixError::SshFailed { host, message } => {
                assert_eq!(host, "mac01");
                assert!(
                    message.contains("tried: 192.0.2.9, mac01.local, mac01, 10.0.0.5"),
                    "{message}"
                );
                assert!(message.contains("as admin"), "{message}");
            }
            other => panic!("expected SshFailed, got {other}"),
        }
        assert_eq!(exec.invocation_count(), 4);
    }

    #[test]
    fn resolve_probes_the_configured_port() {
        let exec = ScriptedExecutor::new().on("", ExecOutput::ok(""));
        let mut n = node("mac01", "mac01");
        n.target_port = Some(2222);
        let r = EndpointResolver::new(&exec, &lookup);
        assert_eq!(r.resolve(&n).unwrap(), "mac01");
        exec.assert_invoked_containing("-l admin -p 2222");
    }
}
