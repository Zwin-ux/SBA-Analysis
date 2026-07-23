"""Run the exact CI gate locally and report evidence.

Mirrors the check steps of .github/workflows/ci.yml (dependency install is
assumed done via `pip install -e ".[dev,ml]"`) so the gate stays runnable
when GitHub-hosted runners are unavailable (for example the account-billing
outage documented in PR #2). Exits non-zero if any step fails.

Usage:
    python scripts/run_ci_gate.py
"""

from __future__ import annotations

import platform
import subprocess
import sys
import time

STEPS: tuple[tuple[str, list[str]], ...] = (
    ("Compile source", [sys.executable, "-m", "compileall", "-q", "src", "tests", "scripts"]),
    ("Run tests", [sys.executable, "-m", "pytest"]),
    ("Run public fixture pipeline", [sys.executable, "scripts/run_sample_pipeline.py"]),
)


def main() -> int:
    print(f"CI gate (local) — Python {platform.python_version()} on {platform.platform()}")
    failures = 0
    for name, cmd in STEPS:
        start = time.perf_counter()
        result = subprocess.run(cmd)
        elapsed = time.perf_counter() - start
        status = "PASS" if result.returncode == 0 else f"FAIL (exit {result.returncode})"
        print(f"[{status}] {name} — {elapsed:.1f}s — {' '.join(cmd)}")
        if result.returncode != 0:
            failures += 1
    if failures:
        print(f"CI gate: {failures} step(s) failed")
        return 1
    print("CI gate: all steps passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
