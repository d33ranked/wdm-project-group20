#!/usr/bin/env python3
"""
Test Suite Runner
=================
Single entry point for all custom tests.

- Asks for TRANSACTION_MODE (TPC or SAGA)
- Updates docker-compose.yml with the chosen mode
- Tears down, rebuilds, and starts all containers
- Waits for services to become healthy
- Runs the appropriate test modules; user presses Enter between test cases

Optional arguments:
  --skip-common         Skip the common test suite, only run mode-specific tests
  --only N [N ...]      Only run the listed test case numbers (1-based) from the
                        mode-specific suite  (e.g. --only 1 3 5)
"""

import argparse
import importlib
import os
import re
import subprocess
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
COMPOSE_FILE = os.path.join(PROJECT_ROOT, "docker-compose.yml")

if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

# When this file runs as __main__, Python creates a separate "run" module
# on import from test files. Alias __main__ → run so both share one namespace.
if __name__ == "__main__":
    sys.modules["run"] = sys.modules["__main__"]

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")

# ---------------------------------------------------------------------------
# Shared test harness — imported by test_common, test_tpc, test_sagas
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Docker helpers (shared by test_tpc and test_sagas)
# ---------------------------------------------------------------------------

def docker_cmd(cmd: str):
    """Run a docker command silently."""
    subprocess.run(
        cmd, shell=True, cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def docker_exec_sql(container: str, db: str, sql: str):
    """Execute a SQL statement inside a postgres container."""
    subprocess.run(
        ["sudo", "docker", "exec", container, "psql", "-U", "user", "-d", db, "-c", sql],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def wait_for_service(probe_path: str, timeout: int = 60):
    """Poll until a service endpoint responds with a non-5xx status."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{BASE_URL}{probe_path}", timeout=3)
            if r.status_code < 500:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


_pass_count = 0
_fail_count = 0


def reset_counts():
    global _pass_count, _fail_count
    _pass_count = 0
    _fail_count = 0


def get_counts():
    return _pass_count, _fail_count


def check(description: str, condition: bool, detail: str = ""):
    """Assert a single test expectation and print result."""
    global _pass_count, _fail_count
    if condition:
        print(f"    \033[92m[PASS]\033[0m {description}")
        _pass_count += 1
    else:
        msg = f"    \033[91m[FAIL]\033[0m {description}"
        if detail:
            msg += f"  ({detail})"
        print(msg)
        _fail_count += 1


def api(method: str, path: str, **kwargs):
    """Fire an HTTP request against the gateway. Returns the Response object."""
    url = f"{BASE_URL}{path}"
    return requests.request(method, url, timeout=30, **kwargs)


def json_field(response, key):
    """Safely extract a JSON field from a response."""
    return response.json().get(key)


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------
def _run(cmd: str, cwd: str = PROJECT_ROOT, check_rc: bool = True):
    """Run a shell command, streaming output."""
    print(f"  $ {cmd}")
    result = subprocess.run(
        cmd,
        shell=True,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if result.stdout.strip():
        for line in result.stdout.strip().splitlines():
            print(f"    {line}")
    if check_rc and result.returncode != 0:
        print(f"\n  Command failed with exit code {result.returncode}")
        sys.exit(1)
    return result


def set_transaction_mode(mode: str):
    """Patch TRANSACTION_MODE and nginx config in docker-compose.yml."""
    with open(COMPOSE_FILE, "r") as f:
        content = f.read()
    # Update TRANSACTION_MODE env var across all services
    updated = re.sub(
        r"(TRANSACTION_MODE=)\w+",
        rf"\g<1>{mode.upper()}",
        content,
    )
    with open(COMPOSE_FILE, "w") as f:
        f.write(updated)
    print(f"  Docker Compose Updated! Transaction Mode: {mode}")


def docker_clean_and_start():
    """Tear down, rebuild, and start all containers."""
    sep = "=" * LINE_WIDTH
    print("\n" + sep)
    print("  Docker Compose Clean & Start")
    print(sep)
    _run("sudo docker compose down -v --remove-orphans")
    _run("sudo docker compose build --quiet")
    _run("sudo docker compose up -d")
    print()


def wait_for_services(timeout: int = 90):
    """Poll the gateway until all three services respond."""
    print("  Waiting for Services to Become Healthy...")
    endpoints = [
        "/stock/item/create/1",
        "/payment/create_user",
        "/orders/create/healthcheck",
    ]
    start = time.time()
    while time.time() - start < timeout:
        try:
            ok = all(
                requests.post(f"{BASE_URL}{ep}", timeout=5).status_code < 300
                for ep in endpoints
            )
            if ok:
                print("  All Services Are Up.\n")
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    print("  Timed Out Waiting for Services.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Pause between tests
# ---------------------------------------------------------------------------
LINE_WIDTH = 60


def wait_for_enter():
    """Pause until the user presses Enter before the next test."""
    input("\n  Press Enter to run the next test... ")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
def collect_tests(module_name: str):
    """Import a sibling module and return its ordered test functions."""
    mod = importlib.import_module(module_name)
    return getattr(mod, "TESTS", [])


def run_suite(label: str, tests: list, only: list[int] | None = None):
    """Run a list of (name, func) test pairs; user presses Enter between them.

    Args:
        label:  Display label for this suite.
        tests:  Full list of (name, func) pairs.
        only:   1-based indices of tests to run. None means run all.
    """
    global _fail_count
    if not tests:
        sep = "=" * LINE_WIDTH
        print(f"\n{sep}")
        print(f"  {label}: no tests defined yet")
        print(sep)
        return 0, 0

    # Apply --only filter if provided
    if only:
        invalid = [n for n in only if n < 1 or n > len(tests)]
        if invalid:
            print(
                f"  \033[91m[ERROR]\033[0m --only contains out-of-range numbers: "
                f"{invalid}  (suite has {len(tests)} tests)"
            )
            sys.exit(1)
        # Keep original 1-based index alongside each test for display
        selected = [(n, tests[n - 1]) for n in sorted(only)]
    else:
        selected = [(n, t) for n, t in enumerate(tests, start=1)]

    sep = "=" * LINE_WIDTH
    dash = "-" * LINE_WIDTH
    skipped = len(tests) - len(selected)
    skip_note = f", skipping {skipped}" if skipped else ""
    print(f"\n{sep}")
    print(f"  {label}  ({len(selected)}/{len(tests)} Test Cases{skip_note})")
    print(sep)

    case_pass = 0
    case_fail = 0

    for run_idx, (original_num, (name, func)) in enumerate(selected):
        if run_idx > 0:
            wait_for_enter()

        reset_counts()
        print(f"\n  [{original_num}/{len(tests)}] {name}")
        print(dash)
        try:
            func()
        except Exception as e:
            print(f"    \033[91m[ERROR]\033[0m {e}")
            _fail_count += 1

        _, f = get_counts()
        if f == 0:
            case_pass += 1
        else:
            case_fail += 1

    return case_pass, case_fail


def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmarking suite runner — Group 20",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python run.py                        # run everything\n"
            "  python run.py --skip-common          # skip common tests\n"
            "  python run.py --only 2 4             # only suite tests 2 and 4\n"
            "  python run.py --skip-common --only 1 3 5\n"
        ),
    )
    parser.add_argument(
        "--skip-common",
        action="store_true",
        help="Skip the common test suite and only run the mode-specific suite.",
    )
    parser.add_argument(
        "--only",
        metavar="N",
        nargs="+",
        type=int,
        help="Only run these test case numbers (1-based) from the mode-specific suite.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    sep = "=" * LINE_WIDTH
    print()
    print(sep)
    print("  BENCHMARKING SUITE — GROUP 20")
    print(sep)

    # --- Ask for mode ---
    print()
    mode = input("  Enter Transaction Mode (TPC / SAGA): ").strip().upper()
    if mode not in ("TPC", "SAGA"):
        print("  Invalid Input. Must explicitly be TPC or SAGA.")
        sys.exit(1)

    # --- Update compose and restart containers ---
    set_transaction_mode(mode)
    docker_clean_and_start()
    wait_for_services()

    # --- Collect tests ---
    common_tests = collect_tests("test_common")
    mode_tests = (
        collect_tests("test_tpc") if mode == "TPC" else collect_tests("test_sagas")
    )
    mode_label = "2PC (Two-Phase Commit)" if mode == "TPC" else "SAGA"

    # --- Run ---
    total_pass, total_fail = 0, 0

    if not args.skip_common:
        p, f = run_suite("Common Test Suite", common_tests)
        total_pass += p
        total_fail += f
        if common_tests and mode_tests:
            wait_for_enter()
    else:
        print(f"\n  [--skip-common] Skipping Common Test Suite.")

    p, f = run_suite(f"{mode_label} Test Suite", mode_tests, only=args.only)
    total_pass += p
    total_fail += f

    # --- Final summary (counts are test cases, not granular checks) ---
    total_cases = total_pass + total_fail
    sep = "=" * LINE_WIDTH
    print(f"\n{sep}")
    print(f"  Final Summary")
    print(sep)
    print(f"    Mode:        {mode}")
    if args.skip_common:
        print(f"    Common:      skipped")
    if args.only:
        print(f"    Only:        {args.only}")
    print(f"    Test cases:  {total_cases}")
    print(f"    Passed:      \033[92m{total_pass}\033[0m")
    print(f"    Failed:      \033[91m{total_fail}\033[0m")
    if total_fail == 0:
        print(f"    Result:      \033[92mALL TESTS PASSED\033[0m")
    else:
        print(f"    Result:      \033[91mSOME TESTS FAILED\033[0m")
    print(sep + "\n")

    sys.exit(0 if total_fail == 0 else 1)


if __name__ == "__main__":
    main()