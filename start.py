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

# CPU resource limit configurations (for services)
# Option 1: No limits - unbounded, can use all cores
# Option 2: Shared core - all containers compete on core 0 with 0.5 CPU each
# Option 3: One core per container - each container type gets exclusive core
_RESOURCE_LIMITS = {
    1: {
        "app_cpus": "0", "app_cpuset": "",
        "redis_cpus": "0", "redis_cpuset": "",
        "sentinel_cpus": "0", "sentinel_cpuset": "",
        "nginx_cpus": "0", "nginx_cpuset": "",
    },
    2: {
        "app_cpus": "0.5", "app_cpuset": "0",
        "redis_cpus": "0.5", "redis_cpuset": "0",
        "sentinel_cpus": "0.5", "sentinel_cpuset": "0",
        "nginx_cpus": "0.5", "nginx_cpuset": "0",
    },
    3: {
        "app_cpus": "1.0", "app_cpuset": "0",
        "redis_cpus": "1.0", "redis_cpuset": "1",
        "sentinel_cpus": "1.0", "sentinel_cpuset": "2",
        "nginx_cpus": "1.0", "nginx_cpuset": "3",
    },
}

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
    # matches docker-compose: gateway :-1, order/stock/payment :-2
    if layout == 1:
        env["GATEWAY_REPLICAS"] = "1"
        env["ORDER_REPLICAS"] = "2"
        env["STOCK_REPLICAS"] = "2"
        env["PAYMENT_REPLICAS"] = "2"
    else:
        env["GATEWAY_REPLICAS"] = str(ask_int("Gateway Service", 1, scoped=True))
        env["ORDER_REPLICAS"] = str(ask_int("Order Service", 2, scoped=True))
        env["STOCK_REPLICAS"] = str(ask_int("Stock Service", 2, scoped=True))
        env["PAYMENT_REPLICAS"] = str(ask_int("Payment Service", 2, scoped=True))


def _apply_resource_limits(env: dict, limits: int) -> None:
    config = _RESOURCE_LIMITS[limits]
    # Application services
    env["SERVICE_CPU_LIMIT"] = config["app_cpus"]
    env["SERVICE_CPUSET"] = config["app_cpuset"]
    # Redis containers
    env["REDIS_CPU_LIMIT"] = config["redis_cpus"]
    env["REDIS_CPUSET"] = config["redis_cpuset"]
    # Sentinel containers
    env["SENTINEL_CPU_LIMIT"] = config["sentinel_cpus"]
    env["SENTINEL_CPUSET"] = config["sentinel_cpuset"]
    # Nginx load balancer
    env["NGINX_CPU_LIMIT"] = config["nginx_cpus"]
    env["NGINX_CPUSET"] = config["nginx_cpuset"]


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
    limits_desc = (
        "Shared Core"
        if env["SERVICE_CPUSET"] == "0"
        else (
            "One Core Per Container"
            if env["SERVICE_CPU_LIMIT"] == "1.0"
            else "No Limits"
        )
    )
    return [
        ("Transaction Mode", env["TRANSACTION_MODE"], "grey"),
        ("Resource Limits", limits_desc, "grey"),
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

        limits = ask_three("Resource Limits?", "No Limits", "Shared Core", "One Core Per Container")
        _apply_resource_limits(env, limits)

        layout = ask("Replicas?", "Compose Defaults", "Custom")
        _apply_replicas(env, layout)

        tune = ask("Pool And Stream Batch?", "Project Defaults", "Custom")
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
