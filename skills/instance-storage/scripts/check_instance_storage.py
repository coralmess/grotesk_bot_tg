from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
INSTANCE_OPS = ROOT / "skills" / "instance-ops" / "scripts" / "instance_ops.py"


def run_remote(command: str) -> str:
    result = subprocess.run(
        [sys.executable, str(INSTANCE_OPS), "exec", "--", "bash", "-lc", command],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip() or "remote command failed"
        raise SystemExit(stderr)
    return result.stdout.rstrip()


def build_command(deep: bool) -> str:
    targets = [
        "/home/ubuntu",
        "/srv",
        "/var",
        "/srv/minecraft-fabric-1.21.11",
        "/home/ubuntu/LystTgFirefox",
    ]
    max_depth = 2 if deep else 1
    targets_str = " ".join(targets)
    return f"""
set -e
echo '== Filesystem =='
df -h /
echo
echo '== Key Paths =='
du -xh -d 0 {targets_str} 2>/dev/null | sort -h
echo
echo '== Breakdown =='
du -xh -d {max_depth} {targets_str} 2>/dev/null | sort -h | tail -n 80
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Check storage usage on the Ubuntu instance.")
    parser.add_argument("--deep", action="store_true", help="Show a deeper breakdown of major directories.")
    args = parser.parse_args()
    print(run_remote(build_command(args.deep)))


if __name__ == "__main__":
    main()
