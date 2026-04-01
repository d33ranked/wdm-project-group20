#!/usr/bin/env python3
"""Interactive Launcher: Docker Compose Stack or Test Suite."""

import os
import subprocess
import sys
from typing import Union

_MIN_PY = (3, 9)

if sys.version_info < _MIN_PY:
    print(
        f"Need Python {_MIN_PY[0]}.{_MIN_PY[1]}+ (this interpreter is "
        f"{sys.version_info.major}.{sys.version_info.minor}).",
        file=sys.stderr,
    )
    sys.exit(1)

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
TEST_RUNNER = os.path.join(PROJECT_ROOT, "test", "run.py")

DEFAULT_REDIS_MAX = 6000
DEFAULT_STREAM_BATCH = 100

_RST = "\033[0m"


def _use_color() -> bool:
    if os.environ.get("NO_COLOR", "").strip():
        return False
    return sys.stdout.isatty()


def _s(code: str, text: str) -> str:
    if not _use_color():
        return text
    return f"\033[{code}m{text}{_RST}"


def _dim(t: str) -> str:
    return _s("2", t)


def _cy(t: str) -> str:
    return _s("36", t)


def _wh(t: str) -> str:
    return _s("97", t)


def _bl(t: str) -> str:
    return _s("34", t)


def _red(t: str) -> str:
    return _s("31", t)


def _yl(t: str) -> str:
    return _s("33", t)


def ask(prompt: str, a: str, b: str) -> int:
    while True:
        s = input(
            f"{_cy(prompt)} {_dim('1=')}{a} {_dim('2=')}{b} {_cy('>')} "
        ).strip()
        if s in ("1", "2"):
            return int(s)
        print(_red("Use 1 Or 2."))


def ask_three(prompt: str, a: str, b: str, c: str) -> int:
    while True:
        s = input(
            f"{_cy(prompt)} {_dim('1=')}{a} {_dim('2=')}{b} {_dim('3=')}{c} {_cy('>')} "
        ).strip()
        if s in ("1", "2", "3"):
            return int(s)
        print(_red("Use 1, 2, Or 3."))


_SUMMARY_INDENT = "  "


def ask_int(label: str, default: int, *, scoped: bool = False) -> int:
    hint = _dim(f"(Press Enter For Default [{default}])")
    prefix = _SUMMARY_INDENT if scoped else ""
    if scoped:
        line = f"{prefix}{_wh(label)} {hint} {_wh('>')} "
    else:
        line = f"{prefix}{_cy(label)} {hint} {_cy('>')} "
    while True:
        s = input(line).strip()
        if not s:
            return default
        if s.isdigit() and int(s) >= 1:
            return int(s)
        err = _red("Enter A Positive Integer, Or Press Enter For Default.")
        print(f"{prefix}{err}" if scoped else err)


def run(cmd: str, env: dict) -> None:
    print(f"{_dim('$')} {_dim(cmd)}")
    r = subprocess.run(cmd, shell=True, cwd=PROJECT_ROOT, env=env)
    if r.returncode != 0:
        print(_red(f"Exit {r.returncode}."))
        sys.exit(r.returncode)


def env_for_mode(mode: str) -> dict:
    e = os.environ.copy()
    e["TRANSACTION_MODE"] = mode
    e["NGINX_CONF"] = (
        "gateway_nginx_saga.conf" if mode == "SAGA" else "gateway_nginx.conf"
    )
    return e


def _apply_replicas(env: dict, layout: int) -> None:
    if layout == 1:
        # TODO: Code duplication with the defaults in docker-compose.yml
        env["GATEWAY_REPLICAS"] = "1"
        env["ORDER_REPLICAS"] = "2"
        env["STOCK_REPLICAS"] = "2"
        env["PAYMENT_REPLICAS"] = "2"
    elif layout == 2:
        # We have 50 CPUs at our disposal
        # Fixed containers (12 total): TODO: Adjust this as necessary
        #   nginx, redis-order, redis-stock, redis-payment, redis-bus,
        #   redis-order-replica, redis-stock-replica, redis-payment-replica,
        #   redis-bus-replica, sentinel-1, sentinel-2, sentinel-3
        # Remaining 38 CPUs for scalable services
        # TODO: Tune these values to get better performance
        # TODO: Each service replica should have its own DB cluster, so this would need to be
        # adjusted once we fix that.
        env["GATEWAY_REPLICAS"] = "4"
        env["ORDER_REPLICAS"] = "10"
        env["STOCK_REPLICAS"] = "12"
        env["PAYMENT_REPLICAS"] = "12"
        
    else:
        env["GATEWAY_REPLICAS"] = str(ask_int("Gateway Service", 1, scoped=True))
        env["ORDER_REPLICAS"] = str(ask_int("Order Service", 2, scoped=True))
        env["STOCK_REPLICAS"] = str(ask_int("Stock Service", 2, scoped=True))
        env["PAYMENT_REPLICAS"] = str(ask_int("Payment Service", 2, scoped=True))

def _apply_resource_limits(env: dict, limits: int) -> None:
    fixed_containers = [
        "NGINX",
        "REDIS_ORDER",
        "REDIS_STOCK",
        "REDIS_PAYMENT",
        "REDIS_BUS",
        "REDIS_ORDER_REPLICA",
        "REDIS_STOCK_REPLICA",
        "REDIS_PAYMENT_REPLICA",
        "REDIS_BUS_REPLICA",
        "SENTINEL_1",
        "SENTINEL_2",
        "SENTINEL_3",
    ]
    replicated_containers = [
        "GATEWAY",
        "ORDER",
        "STOCK",
        "PAYMENT",
    ]
    if limits == 1:
        env["RESOURCE_LIMITS_DESCRIPTION"] = "No Limits"
        env[f"CPU_LIMIT"] = "0"
        for container in fixed_containers + replicated_containers:
            env[f"{container}_CPUSET"] = ""
    elif limits == 2:
        env["RESOURCE_LIMITS_DESCRIPTION"] = "Shared Core"
        env[f"CPU_LIMIT"] = "0"
        for container in fixed_containers + replicated_containers:
            env[f"{container}_CPUSET"] = "0"
    else:
        env["RESOURCE_LIMITS_DESCRIPTION"] = "One Core Per Container"
        env[f"CPU_LIMIT"] = "1"
        curr_cpu = 0
        for fixed_container in fixed_containers:
            env[f"{fixed_container}_CPUSET"] = str(curr_cpu)
            curr_cpu += 1
        for replicated_container in replicated_containers:
            num_replicas = int(env[f"{replicated_container}_REPLICAS"])
            env[f"{replicated_container}_CPUSET"] = f"{curr_cpu}-{curr_cpu + num_replicas - 1}" # - 1 at the end, since both ranges are inclusive
            curr_cpu += num_replicas


def _apply_stream_tuning(env: dict, tune: int) -> None:
    if tune == 1:
        env["REDIS_MAX_CONNECTIONS"] = str(DEFAULT_REDIS_MAX)
        env["STREAM_BATCH_SIZE"] = str(DEFAULT_STREAM_BATCH)
    else:
        env["REDIS_MAX_CONNECTIONS"] = str(
            ask_int("Pool Max Connections", DEFAULT_REDIS_MAX, scoped=True)
        )
        env["STREAM_BATCH_SIZE"] = str(
            ask_int("Stream Batch Size", DEFAULT_STREAM_BATCH, scoped=True)
        )


def _print_summary_rows(
    rows: list[Union[tuple[str, str], tuple[str, str, str]]],
) -> None:
    print()
    keys = [r[0] for r in rows]
    lw = max(len(k) for k in keys)
    for row in rows:
        key, val = row[0], row[1]
        tone = row[2] if len(row) > 2 else "blue"
        v = _dim(val) if tone == "grey" else _bl(val)
        print(f"{_SUMMARY_INDENT}{_wh(key.ljust(lw))}  {v}")
    print()


def _summary_base_rows(env: dict) -> list:
    return [
        ("Transaction Mode", env["TRANSACTION_MODE"], "grey"),
        ("Resource Limits", env["RESOURCE_LIMITS_DESCRIPTION"], "grey"),
        ("Gateway Service", env["GATEWAY_REPLICAS"]),
        ("Order Service", env["ORDER_REPLICAS"]),
        ("Stock Service", env["STOCK_REPLICAS"]),
        ("Payment Service", env["PAYMENT_REPLICAS"]),
        ("Pool Max Connections", env["REDIS_MAX_CONNECTIONS"]),
        ("Stream Batch Size", env["STREAM_BATCH_SIZE"]),
    ]


def teardown(env: dict) -> None:
    print(_dim("Tearing Down Stack…"))
    subprocess.run(
        "docker compose down -v --remove-orphans",
        shell=True,
        cwd=PROJECT_ROOT,
        env=env,
    )


def main() -> None:
    try:
        mode = "TPC" if ask("Mode?", "TPC", "SAGA") == 1 else "SAGA"
        action = ask("Action?", "Start Stack", "Run Tests")

        env = env_for_mode(mode)

        layout = ask_three("Replicas?", "Defaults", "Optimized 50 CPUs", "Custom")
        _apply_replicas(env, layout)

        limits = ask_three("Resource Limits?", "No Limits", "Shared Core", "One Core Per Container")
        _apply_resource_limits(env, limits)

        tune = ask("Pool And Stream Batch?", "Defaults", "Custom")
        _apply_stream_tuning(env, tune)

        if action == 1:
            _print_summary_rows(_summary_base_rows(env))
            print(_dim("Docker Compose Will Reset Volumes, Build Images, And Start Containers."))
            run("docker compose down -v --remove-orphans", env)
            run("docker compose build --quiet", env)
            run("docker compose up -d", env)
            print(f"{_dim('Ready:')} {_bl('http://localhost:8000')}")

        else:
            skip = ask("Skip Build?", "Yes", "No") == 1
            env.pop("DDM_SKIP_BUILD", None)
            if skip:
                env["DDM_SKIP_BUILD"] = "1"

            rows = _summary_base_rows(env)
            rows.insert(1, ("Skip Image Build", "Yes" if skip else "No", "grey"))
            _print_summary_rows(rows)
            print(_dim("Test Suite Starts Next; Stack Is Torn Down When It Finishes."))

            result = subprocess.run(
                [sys.executable, TEST_RUNNER],
                cwd=PROJECT_ROOT,
                env=env,
            )
            teardown(env)
            sys.exit(result.returncode)

    except KeyboardInterrupt:
        print(_yl("Cancelled."))
        sys.exit(130)


if __name__ == "__main__":
    main()
