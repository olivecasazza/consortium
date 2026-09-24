//! The reporting contract every integration satisfies.
//!
//! Each integration crate returns its own report type from its top-level
//! entry points (e.g. `consortium_nix::DeployReport`, or the core DAG
//! executor's [`DagReport`]). [`IntegrationReport`] is the common denominator
//! the contract-test harness and higher-level tooling code against, so
//! "did it work, how many failed" is answered uniformly.

use consortium::dag::DagReport;

/// Uniform success/failure accounting for integration run reports.
pub trait IntegrationReport {
    /// Whether the run had no failures.
    fn is_success(&self) -> bool;
    /// Number of failed units of work (hosts, jobs, tasks — integration-specific).
    fn failure_count(&self) -> usize;
    /// Number of successful units of work (including skipped-but-satisfied ones).
    fn success_count(&self) -> usize;
}

impl IntegrationReport for DagReport {
    fn is_success(&self) -> bool {
        self.failed.is_empty()
    }

    fn failure_count(&self) -> usize {
        self.failed.len()
    }

    fn success_count(&self) -> usize {
        self.completed.len() + self.skipped.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consortium::dag::TaskId;
    use std::collections::{HashMap, HashSet};

    fn report(
        completed: &[&str],
        skipped: &[&str],
        failed: &[(&str, &str)],
        cancelled: &[&str],
    ) -> DagReport {
        DagReport {
            completed: completed.iter().map(|s| TaskId(s.to_string())).collect(),
            skipped: skipped
                .iter()
                .map(|s| TaskId(s.to_string()))
                .collect::<HashSet<_>>(),
            failed: failed
                .iter()
                .map(|(k, v)| (TaskId(k.to_string()), v.to_string()))
                .collect::<HashMap<_, _>>(),
            cancelled: cancelled.iter().map(|s| TaskId(s.to_string())).collect(),
        }
    }

    #[test]
    fn dag_report_all_success() {
        let r = report(&["a", "b"], &["c"], &[], &[]);
        assert!(IntegrationReport::is_success(&r));
        assert_eq!(IntegrationReport::failure_count(&r), 0);
        assert_eq!(IntegrationReport::success_count(&r), 3);
    }

    #[test]
    fn dag_report_with_failures() {
        let r = report(&["a"], &[], &[("b", "boom"), ("c", "kaput")], &["d"]);
        assert!(!IntegrationReport::is_success(&r));
        assert_eq!(IntegrationReport::failure_count(&r), 2);
        assert_eq!(IntegrationReport::success_count(&r), 1);
        // cancelled tasks count as neither success nor failure
    }

    #[test]
    fn dag_report_empty() {
        let r = report(&[], &[], &[], &[]);
        assert!(IntegrationReport::is_success(&r));
        assert_eq!(IntegrationReport::failure_count(&r), 0);
        assert_eq!(IntegrationReport::success_count(&r), 0);
    }
}
