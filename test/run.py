#!/usr/bin/env python3
# test runner — single command, no prompts
# usage: python run.py [--mode TPC|SAGA] [--skip-build] [--no-restart]

import argparse
import importlib
import os
import subprocess
import sys
import time

import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

# alias __main__ → run so sibling modules share this namespace when imported
if __name__ == "__main__":
    sys.modules["run"] = sys.modules["__main__"]

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")
MODE = "TPC"  # set by main() before any suite runs

# --- ansi colours ---
_BLUE = "\033[94m"
_GREEN = "\033[92m"
_RED = "\033[91m"
_RESET = "\033[0m"

# --- pass/fail counters ---
_pass_count = 0
_fail_count = 0


def reset_counts():
    global _pass_count, _fail_count
    _pass_count = 0
    _fail_count = 0


def get_counts():
    return _pass_count, _fail_count


def check(description: str, condition: bool, detail: str = ""):
    # assert one expectation and print pass/fail
    global _pass_count, _fail_count
    if condition:
        print(f"      {_GREEN}[PASS]{_RESET} {description}")
        _pass_count += 1
    else:
        msg = f"      {_RED}[FAIL]{_RESET} {description}"
        if detail:
            msg += f"  ({detail})"
        print(msg)
        _fail_count += 1


def api(method: str, path: str, **kwargs):
    # fire http request against gateway; returns Response
    return requests.request(method, f"{BASE_URL}{path}", timeout=30, **kwargs)


def json_field(response, key):
    # safely extract json field from response
    return response.json().get(key)


def docker_cmd(cmd: str):
    # run docker command silently
    subprocess.run(
        cmd,
        shell=True,
        cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def docker_exec_redis(container: str, *cmd_args):
    # execute redis-cli inside container; args passed directly, no shell splitting
    subprocess.run(
        ["docker", "exec", container, "redis-cli"] + list(cmd_args),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def get_redis_master_container(master_name: str) -> str:
    # ask sentinel-1 which container is currently master for master_name.
    # with announce-hostnames yes, sentinel returns the service hostname
    # (e.g. "redis-stock-replica") after a failover, not the original primary.
    # falls back to the original primary container if sentinel is unreachable.
    result = subprocess.run(
        [
            "docker", "exec", "wdm-project-group24-sentinel-1-1",
            "redis-cli", "-p", "26379",
            "SENTINEL", "GET-MASTER-ADDR-BY-NAME", master_name,
        ],
        capture_output=True,
        text=True,
    )
    lines = [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]
    if lines:
        hostname = lines[0]  # e.g. "redis-stock" or "redis-stock-replica"
        return f"wdm-project-group24-{hostname}-1"
    return f"wdm-project-group24-{master_name}-1"


def wait_for_service(probe_path: str, timeout: int = 60):
    # poll until endpoint returns non-5xx
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


# --- docker stack ---


def _compose_env(mode: str) -> dict:
    # build subprocess env with mode-specific vars, inheriting host env
    env = os.environ.copy()
    env["TRANSACTION_MODE"] = mode
    env["NGINX_CONF"] = (
        "gateway_nginx_saga.conf" if mode == "SAGA" else "gateway_nginx.conf"
    )
    return env


def _run(cmd: str, env: dict | None = None):
    # run shell command with native TTY output (preserves Docker animations)
    print(f"  $ {cmd}")
    result = subprocess.run(
        cmd,
        shell=True,
        cwd=PROJECT_ROOT,
        env=env,
    )
    if result.returncode != 0:
        print(f"\n  command failed (exit {result.returncode})")
        sys.exit(1)
    return result


def start_stack(mode: str, skip_build: bool = False, no_restart: bool = False):
    # bring stack up in correct mode; optionally skip rebuild or full restart
    env = _compose_env(mode)
    print(f"\n  Stack  Mode={mode}  SkipBuild={skip_build}  NoRestart={no_restart}\n")
    if not no_restart:
        _run("docker compose down -v --remove-orphans", env=env)
    if not skip_build:
        _run("docker compose build --quiet", env=env)
    _run("docker compose up -d", env=env)
    print()


def wait_for_services(timeout: int = 90):
    # poll gateway until all three services respond
    print("  Waiting For Services...")
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
                print("  Services Ready\n")
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    print("  Timed Out Waiting For Services")
    sys.exit(1)


# --- test runner ---


def run_suite(label: str, module_name: str) -> tuple[int, int]:
    # import module, run all TESTS entries without prompts; TESTS is (container, name, func)
    global _fail_count
    mod = importlib.import_module(module_name)
    tests = getattr(mod, "TESTS", [])

    if not tests:
        print(f"\n  {label} (No Tests Defined)\n")
        return 0, 0

    print(f"\n  {label} ({len(tests)} Cases)\n")

    case_pass = 0
    case_fail = 0

    for idx, (container, name, func) in enumerate(tests):
        reset_counts()
        print(f"    [{idx+1}/{len(tests)}] {_BLUE}[{container}]{_RESET} {name}")
        try:
            func()
        except Exception as e:
            print(f"      {_RED}[ERROR]{_RESET} {e}")
            _fail_count += 1
        print()

        _, f = get_counts()
        if f == 0:
            case_pass += 1
        else:
            case_fail += 1

    return case_pass, case_fail


def main():
    parser = argparse.ArgumentParser(description="WDM group 24 test runner")
    parser.add_argument(
        "--mode",
        choices=["TPC", "SAGA"],
        default="TPC",
        help="transaction mode (default: TPC)",
    )
    parser.add_argument(
        "--skip-build", action="store_true", help="skip docker compose build"
    )
    parser.add_argument(
        "--no-restart",
        action="store_true",
        help="skip docker compose down/up (reuse running stack)",
    )
    args = parser.parse_args()

    global MODE
    MODE = args.mode

    print(f"\n  GROUP 20 / MODE: {MODE}\n")

    start_stack(MODE, skip_build=args.skip_build, no_restart=args.no_restart)
    wait_for_services()

    total_pass, total_fail = 0, 0

    p, f = run_suite("COMMON SUITE", "test_common")
    total_pass += p
    total_fail += f

    if MODE == "TPC":
        p, f = run_suite("TWO PHASE COMMIT SUITE", "test_tpc")
    else:
        p, f = run_suite("SAGA SUITE", "test_sagas")
    total_pass += p
    total_fail += f

    # replication suite runs last — it kills primaries and changes sentinel topology,
    # which would break direct-redis writes in earlier suites if run first
    p, f = run_suite("REPLICATION SUITE", "test_replication")
    total_pass += p
    total_fail += f

    total_cases = total_pass + total_fail
    result_label = (
        f"{_GREEN}All Tests Passed{_RESET}"
        if total_fail == 0
        else f"{_RED}Some Tests Failed{_RESET}"
    )

    print(
        f"  BENCHMARK SUMMARY  MODE: {MODE}  CASES: {total_cases}  "
        f"{_GREEN}PASSED: {total_pass}{_RESET}  {_RED}FAILED: {total_fail}{_RESET}"
    )
    print(f"  {result_label}\n")

    sys.exit(0 if total_fail == 0 else 1)


if __name__ == "__main__":
    main()