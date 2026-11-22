use super::{classify, Reply};
use consortium_integration::exec::ExecOutput;

const BANNER: &str = "Job 'raysubmit_abc123' submitted successfully";

// The SDK wraps the HTTP response text in a RuntimeError; Ray's request
// validator supplied the *second* traceback before the head forwarded the job.
const INVALID_REQUEST: &str = concat!(
    "Traceback (most recent call last):\n",
    "  File \"/venv/ray/dashboard/modules/job/sdk.py\", line 250, in submit_job\n",
    "    self._raise_error(r)\n",
    "  File \"/venv/ray/dashboard/modules/dashboard_sdk.py\", line 277, in _raise_error\n",
    "    raise RuntimeError(\n",
    "RuntimeError: Request failed with status code 400: Traceback (most recent call last):\n",
    "  File \"/ray/dashboard/modules/job/utils.py\", line 135, in parse_and_validate_request\n",
    "    return validate_request_type(json_data, request_type)\n",
    "  File \"/ray/dashboard/modules/job/common.py\", line 389, in validate_request_type\n",
    "    return request_type(**json_data)\n",
    "TypeError: JobSubmitRequest.__init__() got an unexpected keyword argument 'unknown_field'\n.\n",
);

#[test]
fn recognizes_the_success_banner_in_both_ray_logging_styles() {
    for stdout in [
        format!("{BANNER}\n  ray job logs raysubmit_abc123\n"),
        format!("2025-06-10 14:05:36,002\tSUCC cli.py:63 -- {BANNER}\n"),
        format!("{BANNER}\n{BANNER}\n"),
        format!("\x1b[38;5;77m{BANNER}\x1b[39m\n"),
    ] {
        assert_eq!(
            classify(&ExecOutput::ok(&stdout)),
            Reply::Accepted("raysubmit_abc123"),
            "{stdout:?}"
        );
    }
}

#[test]
fn a_post_submit_version_check_can_fail_after_ray_prints_its_receipt() {
    let output = ExecOutput::new(1, BANNER, "ConnectionError while getting Ray version");
    assert_eq!(classify(&output), Reply::Accepted("raysubmit_abc123"));
}

#[test]
fn candidates_or_incomplete_banners_are_not_receipts() {
    for stdout in [
        "raysubmit_candidate",
        "Job submitted successfully\nraysubmit_candidate",
        "  ray job status raysubmit_candidate",
        "entrypoint: echo Job 'raysubmit_candidate' submitted successfully",
        "2025-06-10 14:05:36,002\tINFO cli.py:63 -- Job 'raysubmit_candidate' submitted successfully",
        "entrypoint\tSUCC cli.py:63 -- Job 'raysubmit_candidate' submitted successfully",
        "Job 'raysubmit_' submitted successfully",
        "Job 'raysubmit_bad id' submitted successfully",
        "Job 'raysubmit_bad\x1b[0mid' submitted successfully",
        "Job 'raysubmit_candidate' submitted success",
    ] {
        assert_eq!(
            classify(&ExecOutput::ok(stdout)),
            Reply::Unknown,
            "{stdout:?}"
        );
    }
    assert_eq!(classify(&ExecOutput::new(1, "", BANNER)), Reply::Unknown);
}

#[test]
fn conflicting_banners_do_not_select_a_job_to_poll() {
    let stdout = format!("{BANNER}\nJob 'raysubmit_second' submitted successfully\n");
    assert_eq!(classify(&ExecOutput::ok(stdout)), Reply::Unknown);
    assert_eq!(
        classify(&ExecOutput::new(1, BANNER, INVALID_REQUEST)),
        Reply::Unknown
    );
}

#[test]
fn recognizes_ray_request_validation_before_dispatch() {
    for stderr in [
        INVALID_REQUEST,
        INVALID_REQUEST.trim_end_matches(['\n', '.']),
    ] {
        assert_eq!(classify(&ExecOutput::new(1, "", stderr)), Reply::Rejected);
    }
}

#[test]
fn unrelated_or_nested_diagnostics_never_prove_rejection() {
    let nested = format!(
        "Traceback (most recent call last):\nRuntimeError: Request failed with status code 503: {INVALID_REQUEST}"
    );
    for stderr in [
        nested,
        format!("RuntimeError: Request failed with status code 503:\n{INVALID_REQUEST}"),
        INVALID_REQUEST.replace("status code 400:", "status code 500:"),
        INVALID_REQUEST.replace("in submit_job\n", "in _upload_package\n"),
        INVALID_REQUEST.replace(
            "in parse_and_validate_request\n",
            "in submit_job_internal\n",
        ),
        INVALID_REQUEST
            .split_once("TypeError:")
            .unwrap()
            .0
            .to_owned(),
        "RuntimeError: Request failed with status code 400: Bad request.".into(),
        "requests.exceptions.ReadTimeout: HTTPConnectionPool: Read timed out.".into(),
    ] {
        assert_eq!(
            classify(&ExecOutput::new(1, "", &stderr)),
            Reply::Unknown,
            "{stderr:?}"
        );
    }
    assert_eq!(
        classify(&ExecOutput::new(1, INVALID_REQUEST, "")),
        Reply::Unknown
    );
}
