//! Interpret the default (non-`--parsable`) sbatch protocol, not arbitrary SSH output.

use consortium_integration::exec::ExecOutput;

const SUBMITTED: &str = "Submitted batch job ";
const FAILED: &str = "sbatch: error: Batch job submission failed: ";
const PARTITION_DETAIL: &str = "sbatch: error: invalid partition specified: ";

/// OpenSSH's first-contact notice wraps an arbitrary `<host>` (<keytype>)
/// between a fixed prefix and suffix.
const SSH_NOTICE_PREFIX: &str = "Warning: Permanently added ";
const SSH_NOTICE_SUFFIX: &str = " to the list of known hosts.";

/// The wait task can address a single unqualified numeric job ID on the submit node.
/// Upstream sbatch prints `Submitted batch job %u` followed by a newline. Without
/// the terminator, even valid-looking digits might be the prefix of a different ID.
pub(super) fn acknowledged_job_id(output: &ExecOutput) -> Option<u64> {
    if !output.success() {
        return None;
    }
    let line = output.stdout.strip_suffix('\n')?;
    let digits = line
        .strip_suffix('\r')
        .unwrap_or(line)
        .strip_prefix(SUBMITTED)?;
    if !matches!(digits.as_bytes(), [b'1'..=b'9', ..]) {
        return None;
    }
    let id = digits.parse::<u32>().ok()?;
    // A host-key warning is not a contradiction; a second receipt or batch failure is.
    if output.stderr.lines().any(|line| {
        line.starts_with(SUBMITTED)
            || line.starts_with(FAILED)
            || line.starts_with(PARTITION_DETAIL)
    }) {
        return None;
    }
    Some(u64::from(id))
}

/// Recognize the controller's invalid-partition refusal, optionally preceded by
/// its partition detail. Require sbatch's default error status as well: SSH loss,
/// a signal, or a timeout is not a scheduler refusal. OpenSSH first-contact
/// host-key notices are not part of the refusal and are ignored.
pub(super) fn rejected_invalid_partition(output: &ExecOutput) -> bool {
    if output.status != 1 || !output.stdout.is_empty() || !output.stderr.ends_with('\n') {
        return false;
    }
    let mut lines = output
        .stderr
        .lines()
        .filter(|line| !(line.starts_with(SSH_NOTICE_PREFIX) && line.ends_with(SSH_NOTICE_SUFFIX)))
        .rev();
    let reason = lines.next().and_then(|line| line.strip_prefix(FAILED));
    if reason != Some("Invalid partition name specified") {
        return false;
    }
    match lines.next() {
        None => true,
        Some(detail) => {
            detail
                .strip_prefix(PARTITION_DETAIL)
                .is_some_and(|partition| !partition.is_empty())
                && lines.next().is_none()
        }
    }
}
