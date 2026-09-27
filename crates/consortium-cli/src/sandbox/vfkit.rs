//! macOS sandbox backend built on `vfkit` (Virtualization.framework).
//!
//! `vfkit` is a command-line VMM, so this shells out rather than linking a C
//! API. That is deliberate: a process spawn is cheap next to a VM boot, and it
//! keeps `unsafe` FFI out of a published crate.
//!
//! Device syntax is vfkit's own `type,key=value` grammar from its
//! `doc/usage.md`: `-d virtio-fs,sharedDir=<host>,mountTag=<tag>`, mounted in
//! the guest as `mount -t virtiofs <tag> <dir>`.
//!
//! Like libkrun, `vfkit` has no snapshot/restore, so every invocation pays a
//! full guest boot. Read-only grants cannot be expressed (a virtio-fs share is
//! writable), so this backend reports that rather than widening the policy.

use std::path::{Path, PathBuf};
use std::process::Command;

use super::{NetworkPolicy, Sandbox, SandboxCommand, SandboxError, SandboxOutput, SandboxSpec};

/// A macOS backend that runs each command in its own vfkit guest.
#[derive(Debug, Clone)]
pub struct VfkitSandbox {
    binary: PathBuf,
    kernel: PathBuf,
    initrd: PathBuf,
    cmdline: String,
    memory_mib: u32,
    cpus: u32,
}

impl VfkitSandbox {
    /// Build a backend from an explicit `vfkit` binary and a Linux guest.
    pub fn new(
        binary: impl Into<PathBuf>,
        kernel: impl Into<PathBuf>,
        initrd: impl Into<PathBuf>,
        cmdline: impl Into<String>,
    ) -> Self {
        Self {
            binary: binary.into(),
            kernel: kernel.into(),
            initrd: initrd.into(),
            cmdline: cmdline.into(),
            memory_mib: 512,
            cpus: 1,
        }
    }

    /// Set guest memory, in MiB.
    #[must_use]
    pub fn with_memory_mib(mut self, mib: u32) -> Self {
        self.memory_mib = mib;
        self
    }

    /// Set guest vCPU count.
    #[must_use]
    pub fn with_cpus(mut self, cpus: u32) -> Self {
        self.cpus = cpus;
        self
    }

    /// Render the `vfkit` device string for one share.
    fn device_arg(host_path: &Path, mount_tag: &str) -> String {
        format!(
            "virtio-fs,sharedDir={},mountTag={mount_tag}",
            host_path.display()
        )
    }

    /// Build the full argv, or refuse a policy this backend cannot express.
    fn build_argv(
        &self,
        spec: &SandboxSpec,
        command: &SandboxCommand,
    ) -> Result<Vec<String>, SandboxError> {
        if let Some(path) = spec.read_only_paths.first() {
            return Err(SandboxError::UnsupportedReadOnlyShare { path: path.clone() });
        }

        let mut argv = vec![
            "--bootloader".to_string(),
            format!(
                "linux,kernel={},initrd={},cmdline=\"{}\"",
                self.kernel.display(),
                self.initrd.display(),
                self.cmdline
            ),
            "--memory".to_string(),
            self.memory_mib.to_string(),
            "--cpus".to_string(),
            self.cpus.to_string(),
        ];

        for (index, path) in spec.writable_paths.iter().enumerate() {
            argv.push("--device".to_string());
            argv.push(Self::device_arg(path, &format!("share{index}")));
        }

        if spec.network == NetworkPolicy::Deny && !spec.writable_paths.is_empty() {
            return Err(SandboxError::Unsupported("a network-denying policy"));
        }
        argv.push("--device".to_string());
        argv.push("virtio-net".to_string());

        // The guest boots to run exactly this command.
        let payload = std::iter::once(command.program.as_str())
            .chain(command.args.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join(" ");
        argv.push("--cmdline-append".to_string());
        argv.push(payload);

        Ok(argv)
    }
}

impl Sandbox for VfkitSandbox {
    fn name(&self) -> &'static str {
        "vfkit"
    }

    fn is_isolated(&self) -> bool {
        // A real hypervisor boundary: a separate guest kernel under
        // Virtualization.framework, unlike DirectSandbox which shares ours.
        true
    }

    fn exec(
        &self,
        spec: &SandboxSpec,
        command: &SandboxCommand,
    ) -> Result<SandboxOutput, SandboxError> {
        spec.validate()?;
        let argv = self.build_argv(spec, command)?;
        let output = Command::new(&self.binary)
            .args(&argv)
            .stdin(std::process::Stdio::null())
            .output()
            .map_err(SandboxError::Spawn)?;
        Ok(SandboxOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

/// Find `vfkit` on `PATH`.
pub fn which() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join("vfkit"))
        .find(|candidate| candidate.is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend() -> VfkitSandbox {
        VfkitSandbox::new("vfkit", "bzImage", "initrd", "console=ttyS0")
    }

    #[test]
    fn device_string_matches_vfkit_grammar() {
        assert_eq!(
            VfkitSandbox::device_arg(Path::new("/srv/data"), "share0"),
            "virtio-fs,sharedDir=/srv/data,mountTag=share0"
        );
    }

    #[test]
    fn reports_real_isolation() {
        assert!(backend().is_isolated());
        assert_eq!(backend().name(), "vfkit");
    }

    #[test]
    fn read_only_grant_is_refused_not_widened() {
        let spec = SandboxSpec::deny_all().with_read_only("/usr");
        let err = backend()
            .build_argv(&spec, &SandboxCommand::new("/bin/true"))
            .expect_err("must refuse a read-only grant");
        assert!(matches!(err, SandboxError::UnsupportedReadOnlyShare { .. }));
        assert!(err.to_string().contains("read-only"));
    }

    #[test]
    fn writable_share_becomes_a_virtio_fs_device() {
        let spec = SandboxSpec::deny_all()
            .with_writable("/srv/data")
            .with_network(NetworkPolicy::Allow);
        let argv = backend()
            .build_argv(&spec, &SandboxCommand::new("/bin/true"))
            .expect("should build");
        assert!(argv.iter().any(|a| a == "--device"));
        assert!(argv
            .iter()
            .any(|a| a == "virtio-fs,sharedDir=/srv/data,mountTag=share0"));
    }

    #[test]
    fn network_denial_is_refused_rather_than_approximated() {
        let spec = SandboxSpec::deny_all().with_writable("/srv/data");
        let err = backend()
            .build_argv(&spec, &SandboxCommand::new("/bin/true"))
            .expect_err("must refuse");
        assert!(matches!(err, SandboxError::Unsupported(_)));
    }
}
