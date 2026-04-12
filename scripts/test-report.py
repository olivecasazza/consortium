#!/usr/bin/env python3
"""
Automated test report generation script for the consortium project.
Runs Rust and Python tests, collects pass/fail/skip counts, and generates
JSON and markdown reports named by git commit hash.
"""

import subprocess
import json
import re
import os
import sys
import time
from pathlib import Path
from datetime import datetime


def get_report_name():
    """Generate report name based on git commit status."""
    try:
        # Get short commit hash
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True
        )
        commit_hash = result.stdout.strip()
        
        # Check if there are uncommitted changes
        result = subprocess.run(
            ["git", "diff", "--quiet"],
            capture_output=True
        )
        if result.returncode != 0:
            return f"{commit_hash}-dirty"
        
        # Check for uncommitted changes with --cached (staged)
        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            capture_output=True
        )
        if result.returncode != 0:
            return f"{commit_hash}-dirty"
        
        return commit_hash
    except subprocess.CalledProcessError:
        # No git repo or no commits
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"uncommitted-{timestamp}"


def run_rust_tests():
    """Run cargo test and parse results."""
    print("Running Rust tests...")
    result = subprocess.run(
        ["cargo", "test", "--all-features"],
        capture_output=True,
        text=True,
        timeout=600
    )
    output = result.stdout + result.stderr
    
    # Parse test results: "test module::tests::test_name ... ok/FAILED"
    module_counts = {}
    for line in output.split('\n'):
        # Match lines like: "test module::tests::test_name ... ok"
        match = re.match(r'test\s+(\S+?)\s+\.\.\.\s+(ok|FAILED|ignored|PASSED|FAILED)', line)
        if match:
            test_name = match.group(1)
            status = match.group(2)
            
            # Extract module (first part before ::)
            parts = test_name.split('::')
            module = parts[0] if parts else test_name
            
            if module not in module_counts:
                module_counts[module] = {'passed': 0, 'failed': 0, 'skipped': 0}
            
            if status in ('ok', 'PASSED'):
                module_counts[module]['passed'] += 1
            elif status == 'FAILED':
                module_counts[module]['failed'] += 1
            else:  # ignored, etc.
                module_counts[module]['skipped'] += 1
    
    return module_counts


def run_python_tests():
    """Run pytest on each Python test file and parse results."""
    print("Running Python tests...")
    
    # Python test files in the specified order
    python_test_files = [
        "RangeSetTest.py",
        "RangeSetNDTest.py",
        "NodeSetTest.py",
        "NodeSetGroupTest.py",
        "MsgTreeTest.py",
        "DefaultsTest.py",
        "TreeTopologyTest.py",
        "TaskEventTest.py",
        "TaskTimerTest.py",
        "TaskMsgTreeTest.py",
        "TaskLocalTest.py",
        "TaskPortTest.py",
        "TaskRLimitsTest.py",
        "TaskTimeoutTest.py",
        "TaskThreadJoinTest.py",
        "TaskThreadSuspendTest.py",
        "CLIClubakTest.py",
        "CLINodesetTest.py",
        "CLIClushTest.py",
        "CLIConfigTest.py",
        "CLIDisplayTest.py",
        "CLIOptionParserTest.py",
        "MisusageTest.py",
        "StreamWorkerTest.py",
        "WorkerExecTest.py",
        "TreeGatewayTest.py",
        "TreeTaskTest.py",
        "TreeWorkerTest.py",
        "TaskDistantTest.py",
        "TaskDistantPdshTest.py"
    ]
    
    # Build the module name from filename (remove .py)
    def get_module_name(filename):
        return filename[:-3]  # Remove .py extension
    
    # Run maturin develop first
    print("Installing Python bindings with maturin...")
    maturin_result = subprocess.run(
        ["maturin", "develop", "-m", "crates/consortium-py/Cargo.toml"],
        capture_output=True,
        text=True,
        timeout=300
    )
    if maturin_result.returncode != 0:
        print(f"Warning: maturin develop failed: {maturin_result.stderr}")
    
    module_counts = {}
    
    for test_file in python_test_files:
        filepath = Path("tests") / test_file
        if not filepath.exists():
            print(f"  Skipping {test_file}: not found")
            continue
        
        module_name = get_module_name(test_file)
        print(f"  Running {test_file}...")
        
        try:
            result = subprocess.run(
                ["python", "-m", "pytest", str(filepath), "--tb=no", "-q"],
                capture_output=True,
                text=True,
                timeout=60
            )
            output = result.stdout + result.stderr
            
            # Parse summary line
            # Examples:
            # "57 passed in 0.11s"
            # "54 failed, 36 passed, 45 skipped in 7.16s"
            # "1 skipped in 0.05s"
            
            summary_match = re.search(
                r'(\d+)\s+passed(?:,\s+(\d+)\s+failed)?(?:,\s+(\d+)\s+skipped)?',
                output
            )
            
            if summary_match:
                passed = int(summary_match.group(1))
                failed = int(summary_match.group(2)) if summary_match.group(2) else 0
                skipped = int(summary_match.group(3)) if summary_match.group(3) else 0
                
                module_counts[module_name] = {
                    'passed': passed,
                    'failed': failed,
                    'skipped': skipped
                }
            else:
                # Try alternative patterns
                failed_match = re.search(r'(\d+)\s+failed', output)
                passed_match = re.search(r'(\d+)\s+passed', output)
                skipped_match = re.search(r'(\d+)\s+skipped', output)
                
                module_counts[module_name] = {
                    'passed': int(passed_match.group(1)) if passed_match else 0,
                    'failed': int(failed_match.group(1)) if failed_match else 0,
                    'skipped': int(skipped_match.group(1)) if skipped_match else 0
                }
                
        except subprocess.TimeoutExpired:
            print(f"    Timeout running {test_file}")
            module_counts[module_name] = {'passed': 0, 'failed': 0, 'skipped': 0}
        except Exception as e:
            print(f"    Error running {test_file}: {e}")
            module_counts[module_name] = {'passed': 0, 'failed': 0, 'skipped': 0}
    
    return module_counts


def generate_json_report(rust_counts, python_counts, report_name):
    """Generate JSON report."""
    report = {
        'report_name': report_name,
        'generated_at': datetime.now().isoformat(),
        'rust_tests': rust_counts,
        'python_tests': python_counts,
        'summary': {
            'rust_modules': len(rust_counts),
            'python_modules': len(python_counts),
            'total_passed': sum(m['passed'] for m in rust_counts.values()) + sum(m['passed'] for m in python_counts.values()),
            'total_failed': sum(m['failed'] for m in rust_counts.values()) + sum(m['failed'] for m in python_counts.values()),
            'total_skipped': sum(m['skipped'] for m in rust_counts.values()) + sum(m['skipped'] for m in python_counts.values())
        }
    }
    
    return json.dumps(report, indent=2)


def generate_markdown_report(rust_counts, python_counts, report_name):
    """Generate markdown report."""
    lines = []
    
    # Header
    lines.append(f"# Test Report: {report_name}")
    lines.append("")
    lines.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    
    # Summary table
    rust_passed = sum(m['passed'] for m in rust_counts.values())
    rust_failed = sum(m['failed'] for m in rust_counts.values())
    rust_skipped = sum(m['skipped'] for m in rust_counts.values())
    
    python_passed = sum(m['passed'] for m in python_counts.values())
    python_failed = sum(m['failed'] for m in python_counts.values())
    python_skipped = sum(m['skipped'] for m in python_counts.values())
    
    total_passed = rust_passed + python_passed
    total_failed = rust_failed + python_failed
    total_skipped = rust_skipped + python_skipped
    
    lines.append("## Summary")
    lines.append("")
    lines.append("| Category | Passed | Failed | Skipped |")
    lines.append("|----------|--------|--------|---------|")
    lines.append(f"| Rust | {rust_passed} | {rust_failed} | {rust_skipped} |")
    lines.append(f"| Python | {python_passed} | {python_failed} | {python_skipped} |")
    lines.append(f"| **Total** | **{total_passed}** | **{total_failed}** | **{total_skipped}** |")
    lines.append("")
    
    # Rust test details
    if rust_counts:
        lines.append("## Rust Test Results")
        lines.append("")
        lines.append("| Module | Passed | Failed | Skipped |")
        lines.append("|--------|--------|--------|---------|")
        for module, counts in sorted(rust_counts.items()):
            lines.append(f"| {module} | {counts['passed']} | {counts['failed']} | {counts['skipped']} |")
        lines.append("")
    
    # Python test details
    if python_counts:
        lines.append("## Python Test Results")
        lines.append("")
        lines.append("| Module | Passed | Failed | Skipped |")
        lines.append("|--------|--------|--------|---------|")
        for module, counts in sorted(python_counts.items()):
            lines.append(f"| {module} | {counts['passed']} | {counts['failed']} | {counts['skipped']} |")
        lines.append("")
    
    return '\n'.join(lines)


def main():
    # Ensure scripts directory exists
    scripts_dir = Path("scripts")
    scripts_dir.mkdir(exist_ok=True)
    
    # Ensure test-reports directory exists
    reports_dir = Path("test-reports")
    reports_dir.mkdir(exist_ok=True)
    
    # Get report name
    report_name = get_report_name()
    print(f"Report name: {report_name}")
    
    # Run tests
    rust_counts = run_rust_tests()
    print(f"Rust test modules: {len(rust_counts)}")
    
    python_counts = run_python_tests()
    print(f"Python test modules: {len(python_counts)}")
    
    # Generate reports
    json_report = generate_json_report(rust_counts, python_counts, report_name)
    md_report = generate_markdown_report(rust_counts, python_counts, report_name)
    
    # Save reports
    json_path = reports_dir / f"{report_name}.json"
    md_path = reports_dir / f"{report_name}.md"
    
    with open(json_path, 'w') as f:
        f.write(json_report)
    print(f"Saved JSON report: {json_path}")
    
    with open(md_path, 'w') as f:
        f.write(md_report)
    print(f"Saved Markdown report: {md_path}")
    
    # Print summary
    print("\n=== Test Summary ===")
    rust_passed = sum(m['passed'] for m in rust_counts.values())
    rust_failed = sum(m['failed'] for m in rust_counts.values())
    rust_skipped = sum(m['skipped'] for m in rust_counts.values())
    print(f"Rust: {rust_passed} passed, {rust_failed} failed, {rust_skipped} skipped")
    
    python_passed = sum(m['passed'] for m in python_counts.values())
    python_failed = sum(m['failed'] for m in python_counts.values())
    python_skipped = sum(m['skipped'] for m in python_counts.values())
    print(f"Python: {python_passed} passed, {python_failed} failed, {python_skipped} skipped")
    
    total_passed = rust_passed + python_passed
    total_failed = rust_failed + python_failed
    total_skipped = rust_skipped + python_skipped
    print(f"Total: {total_passed} passed, {total_failed} failed, {total_skipped} skipped")
    
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
