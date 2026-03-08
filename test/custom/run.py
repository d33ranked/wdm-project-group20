#!/usr/bin/env python3
"""
Test Suite Runner
=================
Single entry point for all custom tests.

- Asks for TRANSACTION_MODE (TPC or SAGA)
- Updates docker-compose.yml with the chosen mode
- Tears down, rebuilds, and starts all containers
- Waits for services to become healthy
- Runs the appropriate test modules with a 5s countdown between tests
"""

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
        cmd, shell=True, cwd=cwd,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    if result.stdout.strip():
        for line in result.stdout.strip().splitlines():
            print(f"    {line}")
    if check_rc and result.returncode != 0:
        print(f"\n  Command failed with exit code {result.returncode}")
        sys.exit(1)
    return result


def set_transaction_mode(mode: str):
    """Patch TRANSACTION_MODE in docker-compose.yml."""
    with open(COMPOSE_FILE, "r") as f:
        content = f.read()
    updated = re.sub(
        r"(TRANSACTION_MODE=)\w+",
        rf"\g<1>{mode}",
        content,
    )
    with open(COMPOSE_FILE, "w") as f:
        f.write(updated)
    print(f"  docker-compose.yml updated: TRANSACTION_MODE={mode}")


def docker_clean_and_start():
    """Tear down, rebuild, and start all containers."""
    print("\n" + "=" * 60)
    print("  DOCKER: clean → build → start")
    print("=" * 60)
    _run("docker compose down -v --remove-orphans")
    _run("docker compose build --quiet")
    _run("docker compose up -d")
    print()


def wait_for_services(timeout: int = 90):
    """Poll the gateway until all three services respond."""
    print("  Waiting for services to become healthy...")
    endpoints = [
        "/stock/item/create/1",
        "/payment/create_user",
    ]
    start = time.time()
    while time.time() - start < timeout:
        try:
            ok = all(
                requests.post(f"{BASE_URL}{ep}", timeout=5).status_code == 200
                for ep in endpoints
            )
            if ok:
                print("  All services are up.\n")
                return
        except requests.ConnectionError:
            pass
        time.sleep(3)
    print("  Timed out waiting for services.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Countdown between tests
# ---------------------------------------------------------------------------
def countdown(seconds: int = 5):
    """Visual countdown between tests."""
    for i in range(seconds, 0, -1):
        sys.stdout.write(f"\r  Next test in {i}s... ")
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r" + " " * 30 + "\r")
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
def collect_tests(module_name: str):
    """Import a sibling module and return its ordered test functions."""
    mod = importlib.import_module(module_name)
    return getattr(mod, "TESTS", [])


def run_suite(label: str, tests: list):
    """Run a list of (name, func) test pairs with countdown between them."""
    if not tests:
        print(f"\n{'=' * 60}")
        print(f"  {label}: no tests defined yet")
        print(f"{'=' * 60}")
        return 0, 0

    print(f"\n{'=' * 60}")
    print(f"  {label}  ({len(tests)} tests)")
    print(f"{'=' * 60}")

    suite_pass = 0
    suite_fail = 0

    for idx, (name, func) in enumerate(tests):
        if idx > 0:
            countdown()

        reset_counts()
        print(f"\n  [{idx + 1}/{len(tests)}] {name}")
        print(f"  {'-' * 50}")
        try:
            func()
        except Exception as e:
            print(f"    \033[91m[ERROR]\033[0m {e}")
            global _fail_count
            _fail_count += 1

        p, f = get_counts()
        suite_pass += p
        suite_fail += f

    return suite_pass, suite_fail


def main():
    print()
    print("=" * 60)
    print("  WDM Microservices — Custom Test Suite")
    print("=" * 60)

    # --- Ask for mode ---
    print()
    mode = input("  Enter TRANSACTION_MODE [TPC / SAGA]: ").strip().upper()
    if mode not in ("TPC", "SAGA"):
        print("  Invalid mode. Must be TPC or SAGA.")
        sys.exit(1)

    # --- Update compose and restart containers ---
    set_transaction_mode(mode)
    docker_clean_and_start()
    wait_for_services()

    # --- Collect tests ---
    common_tests = collect_tests("test_common")
    mode_tests = collect_tests("test_tpc") if mode == "TPC" else collect_tests("test_sagas")
    mode_label = "2PC (Two-Phase Commit)" if mode == "TPC" else "SAGA"

    # --- Run ---
    total_pass, total_fail = 0, 0

    p, f = run_suite("COMMON TESTS", common_tests)
    total_pass += p
    total_fail += f

    if common_tests and mode_tests:
        countdown()

    p, f = run_suite(f"{mode_label} TESTS", mode_tests)
    total_pass += p
    total_fail += f

    # --- Final summary ---
    print(f"\n{'=' * 60}")
    print(f"  FINAL SUMMARY")
    print(f"{'=' * 60}")
    print(f"    Mode:   {mode}")
    print(f"    Passed: \033[92m{total_pass}\033[0m")
    print(f"    Failed: \033[91m{total_fail}\033[0m")
    if total_fail == 0:
        print(f"    Result: \033[92mALL TESTS PASSED\033[0m")
    else:
        print(f"    Result: \033[91mSOME TESTS FAILED\033[0m")
    print(f"{'=' * 60}\n")

    sys.exit(0 if total_fail == 0 else 1)


if __name__ == "__main__":
    main()
