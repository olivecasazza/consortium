//! Receipts from `ray job submit --no-wait`. The entrypoint's logs are not streamed.
//!
//! The [CLI banner] follows a successful jobs POST. On failure, only the
//! [head's request validation] is identified as a rejection before dispatch.
//!
//! [CLI banner]: https://github.com/ray-project/ray/blob/ray-2.48.0/python/ray/dashboard/modules/job/cli.py#L270-L300
//! [head's request validation]: https://github.com/ray-project/ray/blob/ray-2.48.0/python/ray/dashboard/modules/job/job_head.py#L477-L504

use std::path::Path;

use consortium_integration::exec::ExecOutput;

use crate::cli::{self, decimal};

/// What the captured CLI reply proves, independently of job execution.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Reply<'a> {
    Accepted(&'a str),
    Rejected,
    Unknown,
}

/// Ray prints its success banner after the POST returns the submission ID, but
/// before a final version check. That later call can fail even with `--no-wait`.
pub(super) fn classify(output: &ExecOutput) -> Reply<'_> {
    let mut acknowledged = None;
    for banner in output.stdout.lines().filter_map(banner_start) {
        let Some(id) = banner.strip_suffix("' submitted successfully") else {
            return Reply::Unknown;
        };
        if !valid_job_id(id) || acknowledged.is_some_and(|previous| previous != id) {
            return Reply::Unknown;
        }
        acknowledged = Some(id);
    }

    if request_validation_failed(&output.stderr) {
        return if acknowledged.is_none() && output.status == 1 {
            Reply::Rejected
        } else {
            // A successful exit or a positive banner contradicts this exception.
            Reply::Unknown
        };
    }
    acknowledged.map_or(Reply::Unknown, Reply::Accepted)
}

/// Accept Ray's pretty message or its non-TTY SUCC log record, not command
/// hints, other log levels, or an identifier mentioned somewhere in a line.
fn banner_start(line: &str) -> Option<&str> {
    cli::message(line, "SUCC")?.strip_prefix("Job '")
}

/// Consortium does not pass a caller-selected submission ID to Ray.
fn valid_job_id(id: &str) -> bool {
    id.strip_prefix("raysubmit_").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    })
}

/// The job head returns a 400 traceback from `parse_and_validate_request`
/// *before* it forwards the job. A generic 400 is not enough: Ray also catches
/// errors decoding the agent's response, after the job may already exist.
fn request_validation_failed(stderr: &str) -> bool {
    const TRACEBACK: &str = "Traceback (most recent call last):";
    const BAD_REQUEST: &str = "RuntimeError: Request failed with status code 400: ";
    let mut lines = stderr.lines();
    let start = lines.find(|line| *line == TRACEBACK || line.starts_with("RuntimeError:"));
    if start != Some(TRACEBACK) {
        return false;
    }

    // Inspect the *outer* SDK exception. Nested exceptions in a proxy/5xx
    // response body and errors from other SDK requests are not submit replies.
    let mut frames = ["", ""];
    let message = lines.by_ref().find(|line| {
        if line.starts_with("  File \"") {
            frames = [frames[1], *line];
        }
        !line.starts_with(' ')
    });
    if message.and_then(|line| line.strip_prefix(BAD_REQUEST)) != Some(TRACEBACK)
        || !is_frame(frames[0], "ray/dashboard/modules/job/sdk.py", "submit_job")
        || !is_frame(
            frames[1],
            "ray/dashboard/modules/dashboard_sdk.py",
            "_raise_error",
        )
    {
        return false;
    }

    if !lines.next().is_some_and(|line| {
        is_frame(
            line,
            "ray/dashboard/modules/job/utils.py",
            "parse_and_validate_request",
        )
    }) {
        return false;
    }
    // A frame alone might be the start of a truncated traceback. Require the
    // validation exception, not a traceback from an earlier, handled error.
    let validation = lines
        .find(|line| !line.starts_with(' '))
        .and_then(|line| line.split_once(": "));
    matches!(validation, Some(("TypeError" | "ValueError", reason)) if !reason.is_empty())
        && lines.all(|line| line.is_empty() || line == ".")
}

/// Python installation paths and source line numbers vary between releases.
fn is_frame(line: &str, module: &str, function: &str) -> bool {
    let Some((path, location)) = line
        .strip_prefix("  File \"")
        .and_then(|frame| frame.rsplit_once("\", line "))
    else {
        return false;
    };
    Path::new(path).ends_with(module)
        && location
            .split_once(", in ")
            .is_some_and(|(number, name)| decimal(number) && name == function)
}

#[cfg(test)]
mod tests;
