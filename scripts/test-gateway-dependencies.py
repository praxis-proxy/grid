#!/usr/bin/env python3
"""Verify that cargo-machete analyzes both packages in the nested workspace."""

from pathlib import Path
import shutil
import sys
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

    def test_detects_unused_praxis_filter_alias_without_source_usage(self):
        self.assert_unused_gateway_alias_detected_without_source_usage("praxis-filter")

    def test_detects_unused_praxis_core_alias_without_source_usage(self):
        self.assert_unused_gateway_alias_detected_without_source_usage("praxis-core")

    def test_manifest_analysis_error_fails_the_check(self):
        manifest = self.fixture / "crates/gateway/Cargo.toml"
        with manifest.open("a") as stream:
            stream.write('\n[dependencies.grid-machete-missing]\nworkspace = true\n')
        raw_result = subprocess.run(
            ["cargo", "machete", "crates/gateway"],
            cwd=self.fixture,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        raw_output = raw_result.stdout + raw_result.stderr
        self.assertEqual(raw_result.returncode, 0, raw_output)
        self.assertIn("error when handling", raw_output)

        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts/check-unused-dependencies.py"), "crates/gateway"],
            cwd=self.fixture,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        output = result.stdout + result.stderr
        self.assertNotEqual(result.returncode, 0, output)
        self.assertIn("error when handling", output)

    def assert_unused_gateway_alias_detected_without_source_usage(self, dependency):
        gateway = self.fixture / "crates/gateway"
        for source in gateway.rglob("*.rs"):
            if source.relative_to(gateway).parts[0] != "crates":
                source.unlink()
        main = gateway / "src/main.rs"
        main.parent.mkdir(parents=True, exist_ok=True)
        main.write_text("fn main() {}\n")

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
        self.assertIn(f"\t{dependency}\n", result.stdout, output)

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
