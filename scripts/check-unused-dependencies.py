#!/usr/bin/env python3
"""Run cargo-machete and reject successful exits that skipped manifest analysis."""

import os
import re
import subprocess
import sys


def main():
    """Preserve machete's status, promoting manifest-analysis errors to failures."""
    result = subprocess.run(
        ["cargo", "machete", *sys.argv[1:]],
        capture_output=True,
        text=True,
        env={**os.environ, "CARGO_TERM_COLOR": "never"},
        check=False,
    )
    print(result.stdout, end="")
    print(result.stderr, end="", file=sys.stderr)
    output = re.sub(r"\x1b\[[0-9;]*m", "", result.stdout + result.stderr)
    if result.returncode:
        return result.returncode
    if re.search(r"(?:^|\s)error(?: when |:|\[)", output, re.IGNORECASE):
        print("cargo-machete did not analyze every requested manifest", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
