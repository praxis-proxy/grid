#!/usr/bin/env python3
"""Verify that cargo-machete analyzes both packages in the nested workspace."""

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
UNUSED_DEPENDENCY = "grid-machete-regression-unused"


class GatewayDependencyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="grid-machete-")
        self.addCleanup(temporary.cleanup)
        self.fixture = Path(temporary.name)
        shutil.copy2(ROOT / "Cargo.toml", self.fixture / "Cargo.toml")
        shutil.copytree(
            ROOT / "crates/gateway",
            self.fixture / "crates/gateway",
            ignore=shutil.ignore_patterns("target", ".git", "__pycache__"),
        )

    def test_detects_unused_gateway_dependency(self):
        self.assert_unused_dependency_detected("crates/gateway")

    def test_detects_unused_filter_dependency(self):
        self.assert_unused_dependency_detected("crates/gateway/crates/ai-grid-filters")

    def assert_unused_dependency_detected(self, package):
        manifest = self.fixture / package / "Cargo.toml"
        with manifest.open("a") as stream:
            stream.write(f'\n[dependencies.{UNUSED_DEPENDENCY}]\nversion = "0.0.0"\n')

        result = subprocess.run(
            ["cargo", "machete", "crates/gateway"],
            cwd=self.fixture,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, 1, output)
        self.assertIn(f"\t{UNUSED_DEPENDENCY}\n", result.stdout, output)


if __name__ == "__main__":
    unittest.main()
