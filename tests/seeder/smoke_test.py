#!/usr/bin/env python3
"""Minimal smoke tests for seeder refactor."""

from __future__ import annotations

import glob
import subprocess
import sys


def run(cmd: list[str]) -> None:
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> None:
    run([sys.executable, "apps/seeder/pipeline.py", "--help"])

    has_places = bool(glob.glob("output/seeder/ktb_res*.json"))
    has_menus = bool(glob.glob("output/seeder/menus/*.json"))
    if has_places and has_menus:
        run(
            [
                sys.executable,
                "apps/seeder/pipeline.py",
                "--mode",
                "local",
                "--lat",
                "37.402052",
                "--lng",
                "127.107058",
                "--dry-run",
            ]
        )

    print("seeder smoke test passed")


if __name__ == "__main__":
    main()
