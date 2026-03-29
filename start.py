#!/usr/bin/env python3
"""Interactive Launcher: Docker Compose Stack or Test Suite."""

import os
import subprocess
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
TEST_RUNNER = os.path.join(PROJECT_ROOT, "test", "run.py")

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


_SUMMARY_INDENT = "  "


def ask_int(label: str, default: int, *, replica: bool = False) -> int:
    hint = _dim(f"(Press Enter For Default [{default}])")
    prefix = _SUMMARY_INDENT if replica else ""
    if replica:
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
        print(f"{prefix}{err}" if replica else err)


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


def _print_summary_rows(
    rows: list[tuple[str, str] | tuple[str, str, str]],
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

        if action == 1:
            layout = ask("Replicas?", "2 Each", "Custom")
            if layout == 1:
                for k in (
                    "GATEWAY_REPLICAS",
                    "ORDER_REPLICAS",
                    "STOCK_REPLICAS",
                    "PAYMENT_REPLICAS",
                ):
                    env[k] = "2"
            else:
                env["GATEWAY_REPLICAS"] = str(ask_int("Gateway Service", 1, replica=True))
                env["ORDER_REPLICAS"] = str(ask_int("Order Service", 2, replica=True))
                env["STOCK_REPLICAS"] = str(ask_int("Stock Service", 2, replica=True))
                env["PAYMENT_REPLICAS"] = str(ask_int("Payment Service", 2, replica=True))

            _print_summary_rows(
                [
                    ("Transaction Mode", mode, "grey"),
                    ("Gateway Service", env["GATEWAY_REPLICAS"]),
                    ("Order Service", env["ORDER_REPLICAS"]),
                    ("Stock Service", env["STOCK_REPLICAS"]),
                    ("Payment Service", env["PAYMENT_REPLICAS"]),
                ],
            )
            print(_dim("Docker Compose Will Reset Volumes, Build Images, And Start Containers."))
            run("docker compose down -v --remove-orphans", env)
            run("docker compose build --quiet", env)
            run("docker compose up -d", env)
            print(f"{_dim('Ready:')} {_bl('http://localhost:8000')}")

        else:
            skip = ask("Skip Build?", "Yes", "No") == 1
            for k in (
                "GATEWAY_REPLICAS",
                "ORDER_REPLICAS",
                "STOCK_REPLICAS",
                "PAYMENT_REPLICAS",
            ):
                env[k] = "2"

            _print_summary_rows(
                [
                    ("Transaction Mode", mode, "grey"),
                    ("Skip Image Build", "Yes" if skip else "No", "grey"),
                    ("Gateway Service", env["GATEWAY_REPLICAS"]),
                    ("Order Service", env["ORDER_REPLICAS"]),
                    ("Stock Service", env["STOCK_REPLICAS"]),
                    ("Payment Service", env["PAYMENT_REPLICAS"]),
                ],
            )
            print(_dim("Test Suite Starts Next; Stack Is Torn Down When It Finishes."))

            argv = [sys.executable, TEST_RUNNER, "--mode", mode]
            if skip:
                argv.append("--skip-build")

            result = subprocess.run(argv, cwd=PROJECT_ROOT, env=env)
            teardown(env)
            sys.exit(result.returncode)

    except KeyboardInterrupt:
        print(_yl("Cancelled."))
        sys.exit(130)


if __name__ == "__main__":
    main()
