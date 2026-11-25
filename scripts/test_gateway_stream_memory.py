"""Deterministic tests for the Gateway stream-memory runner contracts."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import signal
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock

from scripts import gateway_stream_memory as memory


def proc_stat(start_ticks: int = 987654) -> str:
    fields = ["S", *(str(index) for index in range(4, 22)), str(start_ticks)]
    return f"123 (gateway name with ) parens) {' '.join(fields)}"


def chunked(body: bytes) -> bytes:
    return f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n\r\n"


class ReviewRegressionTests(unittest.IsolatedAsyncioTestCase):
    def test_ratio_comparison_does_not_round_the_threshold_product(self) -> None:
        self.assertTrue(memory.ratio_exceeded(30 * 1024**2, 40 * 1024**2, "1.3333333333333333333333333333"))
        self.assertFalse(memory.ratio_exceeded(30 * 1024**2, 45 * 1024**2, "1.5"))
        for limit in ["1e9999999", "1e-9999999", "NaN", "Infinity"]:
            with self.assertRaises(ValueError):
                memory.ratio_exceeded(30, 40, limit)

    def test_source_identity_rejects_ignored_cargo_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = memory.Path(directory)
            def git(*args: str) -> None:
                subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)
            git("init", "-q")
            git("config", "user.email", "test@example.test")
            git("config", "user.name", "Test")
            (root / ".dockerignore").write_text("target\n**/target\n")
            (root / ".gitignore").write_text("target/\n.cargo/config\n")
            git("add", ".")
            git("commit", "-qm", "fixture")
            (root / "target").mkdir()
            (root / "target" / "ignored").write_text("excluded by Docker")
            source = memory.git_identity(root)
            (root / ".cargo").mkdir()
            (root / ".cargo" / "config").write_text("[build]\nrustflags=[]\n")
            with self.assertRaisesRegex(memory.MeasurementError, "Git-excluded build input"):
                memory.git_identity(root)
            (root / ".cargo" / "config").unlink()
            self.assertEqual(memory.git_identity(root)["fingerprint_sha256"], source["fingerprint_sha256"])
            override = root / "deploy/gateway/Containerfile.dockerignore"
            override.parent.mkdir(parents=True)
            override.write_text("# root Docker exclusions no longer apply\n")
            with self.assertRaisesRegex(memory.MeasurementError, "Dockerfile-specific ignore"):
                memory.git_identity(root)

    def test_docker_exclusion_proof_is_conservative(self) -> None:
        rules = ["target", "**/target", "*.pem", "crates/web/dist"]
        for path in ["target/", "crates/gateway/target/", "secret.pem", "crates/web/dist/index"]:
            self.assertTrue(memory.definitely_docker_excluded(path, rules), path)
        for path in [".cargo/config", "nested/secret.pem", "crates/targeted/file"]:
            self.assertFalse(memory.definitely_docker_excluded(path, rules), path)
        self.assertFalse(memory.definitely_docker_excluded("target", ["target", "!target/config"]))

    async def test_buffered_data_after_heartbeat_needs_no_additional_chunk(self) -> None:
        payload = b": heartbeat\n\ndata: first\n\n"
        reader = asyncio.StreamReader()
        reader.feed_data(f"{len(payload):x}\r\n".encode() + payload + b"\r\n")
        stream = memory.HttpBody(reader, {"transfer-encoding": "chunked"})
        self.assertEqual(await asyncio.wait_for(stream.next_event(), timeout=0.1), "first")

    async def test_backend_shutdown_closes_connections_before_waiting(self) -> None:
        backend = memory.Backend("shutdown")
        events: list[str] = []
        class Writer:
            def close(self) -> None:
                events.append("writer.close")
        class Server:
            def close(self) -> None:
                events.append("server.close")
            async def wait_closed(self) -> None:
                if "writer.close" not in events:
                    await asyncio.Future()
                events.append("server.wait_closed")
        backend.server = Server()
        backend.writers.add(Writer())
        await asyncio.wait_for(backend.close(), timeout=0.1)
        self.assertEqual(events, ["server.close", "writer.close", "server.wait_closed"])

    async def test_final_idle_sample_window_waits_for_new_phase_samples(self) -> None:
        runner = memory.StreamMemoryRunner.__new__(memory.StreamMemoryRunner)
        sampler = mock.Mock()
        sampler.errors = []
        sampler.phase_samples.side_effect = [[], [{"rss_bytes": 100}] * memory.RSS_SAMPLES]
        runner.sampler = sampler
        with mock.patch.object(memory.asyncio, "sleep", new=mock.AsyncMock()):
            values = await runner.wait_for_samples("final_idle", memory.RSS_SAMPLES)
        self.assertEqual(len(values), memory.RSS_SAMPLES)
        self.assertEqual(sampler.phase_samples.call_args_list, [mock.call("final_idle"), mock.call("final_idle")])

    async def test_profile_uses_recorded_post_close_and_post_deadline_idle_samples(self) -> None:
        for missing_post_close in [False, True]:
            events: list[tuple[str, object]] = []
            runner = memory.StreamMemoryRunner.__new__(memory.StreamMemoryRunner)
            runner.args = memory.argparse.Namespace(mode="full", max_settle_ratio="1.5")
            runner.summary = {}
            runner.bursts = []
            runner.baseline = None
            runner.client_active = set()
            runner.request_tasks = {"burst-01": []}
            runner.warm_gateway = mock.AsyncMock()
            def unrecorded_sample() -> dict:
                raise AssertionError("summary must not perform an unrecorded RSS read")
            runner.gateway_sample = unrecorded_sample
            state = memory.BurstState("burst-01", {"request"})
            state.started = {"request"}
            state.finished = {"request"}
            state.peak_active = state.peak_client_active = 1
            runner.start_burst = mock.AsyncMock(return_value=state)
            class Sampler:
                errors: list = []
                def set_phase(self, phase: str, burst: str | None = None) -> None:
                    events.append(("phase", phase))
                def phase_samples(self, phase: str, burst: str | None = None) -> list[dict]:
                    events.append(("samples", phase))
                    if phase == "burst_post_close" and missing_post_close:
                        return []
                    active = 1 if phase == "burst_live" else 0
                    rss = 123 if phase == "burst_post_close" else 100
                    return [{"rss_bytes": rss, "client_active": active, "backend_active": active}] * memory.RSS_SAMPLES
            runner.sampler = Sampler()
            async def sleep(seconds: float) -> None:
                events.append(("sleep", seconds))
            with mock.patch.object(memory, "FULL_BURSTS", 1), mock.patch.object(memory, "FULL_CONCURRENCY", 1), mock.patch.object(memory.asyncio, "sleep", new=sleep):
                if missing_post_close:
                    with self.assertRaisesRegex(memory.MeasurementError, "post-close RSS sample was not recorded"):
                        await runner.run_memory_profile()
                    continue
                await runner.run_memory_profile()
            self.assertEqual(runner.bursts[0]["immediate_post_close_rss_bytes"], 123)
            wait = events.index(("phase", "final_idle_wait"))
            self.assertEqual(events[wait:wait + 4], [
                ("phase", "final_idle_wait"), ("sleep", 120),
                ("phase", "final_idle"), ("samples", "final_idle"),
            ])
            self.assertEqual(runner.summary["final_idle_samples"], memory.RSS_SAMPLES)

    def test_proc_sample_reads_one_open_process_directory(self) -> None:
        gateway = memory.GatewayContainer.__new__(memory.GatewayContainer)
        gateway.pid = os.getpid()
        gateway.start_ticks = memory.parse_proc_start_ticks(memory.Path(f"/proc/{gateway.pid}/stat").read_text())
        original_open = os.open
        calls: list[tuple[str, dict]] = []
        def record_open(path: str, flags: int, **kwargs: object) -> int:
            calls.append((path, kwargs))
            return original_open(path, flags, **kwargs)
        with mock.patch.object(memory.os, "open", side_effect=record_open):
            sample = gateway.proc_sample()
        directory_fd = calls[1][1]["dir_fd"]
        self.assertEqual(calls[0][0], f"/proc/{gateway.pid}")
        self.assertTrue(all(call[1].get("dir_fd") == directory_fd for call in calls[1:]))
        self.assertEqual(sample["gateway_start_ticks"], gateway.start_ticks)
        self.assertGreater(sample["rss_bytes"], 0)

    def test_preflight_docker_commands_have_timeouts(self) -> None:
        runner = memory.StreamMemoryRunner.__new__(memory.StreamMemoryRunner)
        runner.host = {}
        outputs = [
            subprocess.CompletedProcess([], 0, "29.0", ""),
            subprocess.CompletedProcess([], 0, "/tmp", ""),
            subprocess.CompletedProcess([], 0, "172.17.0.1", ""),
        ]
        runner.args = memory.argparse.Namespace(mode="quick")
        runner.write_manifest = lambda: None
        with mock.patch.object(memory.os, "getuid", return_value=1000), mock.patch.object(memory.os, "getgid", return_value=1000), mock.patch.object(memory.subprocess, "run", side_effect=outputs) as run:
            runner.validate_host()
        self.assertEqual(len(run.call_args_list), 3)
        self.assertTrue(all(call.kwargs.get("timeout") == 30 for call in run.call_args_list))


class ProcParsingTests(unittest.TestCase):
    def test_start_ticks_allow_parentheses_in_process_name(self) -> None:
        self.assertEqual(memory.parse_proc_start_ticks(proc_stat()), 987654)

    def test_start_ticks_reject_missing_field(self) -> None:
        with self.assertRaises(ValueError):
            memory.parse_proc_start_ticks("1 (gateway) S 4 5 6")

    def test_pid_reuse_is_rejected(self) -> None:
        memory.validate_process_start_ticks(7, 7)
        with self.assertRaises(memory.MeasurementError):
            memory.validate_process_start_ticks(8, 7)

    def test_image_provenance_requires_source_variant_and_allocator(self) -> None:
        source = {"commit": "commit", "fingerprint_sha256": "fingerprint"}
        labels = {
            "org.opencontainers.image.revision": "commit",
            "org.grid.qualification.memory.variant": "gnu-jemalloc",
            "org.grid.qualification.memory.allocator": "jemalloc",
            "org.grid.qualification.source-sha256": "fingerprint",
        }
        memory.validate_image_labels(labels, source, "gnu-jemalloc")
        for key, value in (
            ("org.opencontainers.image.revision", "other"),
            ("org.grid.qualification.memory.variant", "musl-malloc"),
            ("org.grid.qualification.memory.allocator", "system"),
            ("org.grid.qualification.source-sha256", "other"),
        ):
            with self.subTest(label=key):
                changed = dict(labels, **{key: value})
                with self.assertRaises(memory.MeasurementError):
                    memory.validate_image_labels(changed, source, "gnu-jemalloc")

    def test_immutable_image_reference_rejects_mutable_tag(self) -> None:
        memory.validate_immutable_image_reference("sha256:image-id", "sha256:image-id", [])
        memory.validate_immutable_image_reference("gateway@sha256:repo-digest", "sha256:image-id", ["gateway@sha256:repo-digest"])
        with self.assertRaises(memory.MeasurementError):
            memory.validate_immutable_image_reference("gateway:latest", "sha256:image-id", [])
        self.assertTrue(memory.is_safe_immutable_image_reference("sha256:" + "a" * 64))
        self.assertTrue(memory.is_safe_immutable_image_reference("ghcr.io/org/image@sha256:" + "b" * 64))
        self.assertFalse(memory.is_safe_immutable_image_reference("https://user:secret@registry/image:tag"))

    def test_unlimited_file_descriptor_limit_is_sufficient(self) -> None:
        memory.validate_file_descriptor_limit(memory.resource.RLIM_INFINITY)
        with self.assertRaises(memory.MeasurementError):
            memory.validate_file_descriptor_limit(1024)

    def test_measurement_user_is_nonroot_and_matches_host_identity(self) -> None:
        self.assertEqual(memory.measurement_user(1000, 1000), "1000:1000")
        with self.assertRaises(memory.MeasurementError):
            memory.measurement_user(0, 0)
        with self.assertRaises(memory.MeasurementError):
            memory.validate_gateway_user("Uid:\t0 0 0 0\nGid:\t0 0 0 0\n", 0, 0)
        memory.validate_gateway_user("Uid:\t1000 1000 1000 1000\nGid:\t1000 1000 1000 1000\n", 1000, 1000)

    def test_cpu_set_parser_accepts_ids_and_ranges(self) -> None:
        self.assertEqual(memory.parse_cpu_set("0-2,5,7-8"), {0, 1, 2, 5, 7, 8})
        with self.assertRaises(ValueError):
            memory.parse_cpu_set("0-x")
        with self.assertRaises(ValueError):
            memory.parse_cpu_set("4-2")

    def test_smaps_rollup_is_preferred(self) -> None:
        rss, source = memory.parse_process_rss("Rss: 12 kB\n", "VmRSS: 99 kB\n")
        self.assertEqual((rss, source), (12 * 1024, "smaps_rollup:Rss"))

    def test_status_is_a_documented_fallback(self) -> None:
        rss, source = memory.parse_process_rss("Rss: broken kB\n", "VmRSS: 99 kB\n")
        self.assertEqual((rss, source), (99 * 1024, "status:VmRSS"))

    def test_malformed_rss_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            memory.parse_process_rss(None, "VmRSS: 1 MB\n")


class NumericGateTests(unittest.TestCase):
    def test_large_integer_ratio_does_not_overflow(self) -> None:
        baseline = 10**100
        final = baseline * 3
        self.assertEqual(memory.ratio_value(baseline, final), memory.decimal.Decimal(3))
        self.assertFalse(memory.ratio_exceeded(baseline, final, "3"))
        self.assertTrue(memory.ratio_exceeded(baseline, final + 1, "3"))

    def test_ratio_rejects_invalid_inputs(self) -> None:
        for baseline, final, limit in ((0, 1, "3"), (1, -1, "3"), (1, 1, "NaN"), (1, 1, "0")):
            with self.subTest(baseline=baseline, final=final, limit=limit):
                with self.assertRaises(ValueError):
                    memory.ratio_exceeded(baseline, final, limit)

    def test_median_keeps_large_integer_precision(self) -> None:
        lower = 10**100
        self.assertEqual(memory.median_bytes([lower, lower + 1]), lower)
        self.assertEqual(memory.median_bytes([4, 8]), 6)
        with self.assertRaises(ValueError):
            memory.median_bytes([])

    def test_full_evidence_requires_real_concurrency_and_cleanup(self) -> None:
        burst = {
            "burst_id": "burst-01",
            "requested_streams": 1024,
            "backend_started": 1024,
            "backend_peak_live": 1024,
            "client_peak_live": 1024,
            "backend_finished": 1024,
            "backend_cancelled": 0,
            "invalid_backend_receipts": [],
            "backend_active_after_drain": 0,
            "client_active_after_drain": 0,
            "live_plateau_sample_count": 5,
            "settle_sample_count": 10,
        }
        bursts = [dict(burst, burst_id=f"burst-{index + 1:02d}") for index in range(memory.FULL_BURSTS)]
        cleanup = {
            "container_name": "grid-memory-run-gnu",
            "container_created": True,
            "stop_return_code": 0,
            "post_stop_running": False,
            "container_image_id": "sha256:image",
            "oom_killed": False,
            "exit_code": 0,
            "remove_return_code": 0,
            "post_remove_absent": True,
            "cleanup_errors": [],
            "gateway_pid": 101,
            "gateway_start_ticks": 9876,
            "startup_identity": "starting grid-gateway",
            "cpuset": "0,1,2,3",
            "memory_limit_bytes": memory.MEMORY_LIMIT_BYTES,
        }
        self.assertEqual(
            memory.validate_full_evidence(
                baseline=100,
                baseline_sample_count=10,
                final_rss=250,
                final_idle_sample_count=10,
                bursts=bursts,
                sample_count=200,
                rss_sources=["smaps_rollup:Rss"] * 200,
                ratio_limit="3",
                require_ratio=True,
                expected_image_id="sha256:image",
                cleanup=cleanup,
            ),
            [],
        )
        for label, sources, expected in (
            ("uniform smaps", ["smaps_rollup:Rss"] * 200, []),
            ("uniform fallback", ["status:VmRSS"] * 200, []),
            ("mixed", ["smaps_rollup:Rss"] * 199 + ["status:VmRSS"],
             ["full evidence changes RSS measurement source during the run"]),
            ("missing", [None] * 200,
             ["full evidence contains a missing or unsupported RSS measurement source"]),
            ("unsupported", ["cgroup:memory.current"] * 200,
             ["full evidence contains a missing or unsupported RSS measurement source"]),
            ("count mismatch", ["smaps_rollup:Rss"] * 199,
             ["full evidence RSS source count does not match the recorded sample count"]),
        ):
            with self.subTest(rss_sources=label):
                self.assertEqual(
                    memory.validate_full_evidence(
                        baseline=100,
                        baseline_sample_count=10,
                        final_rss=250,
                        final_idle_sample_count=10,
                        bursts=bursts,
                        sample_count=200,
                        rss_sources=sources,
                        ratio_limit="3",
                        require_ratio=True,
                        expected_image_id="sha256:image",
                        cleanup=cleanup,
                    ),
                    expected,
                )
        partial_bursts = [dict(item) for item in bursts]
        partial_bursts[0] = dict(partial_bursts[0], backend_peak_live=4)
        errors = memory.validate_full_evidence(
            baseline=100,
            baseline_sample_count=10,
            final_rss=250,
            final_idle_sample_count=10,
            bursts=partial_bursts,
            sample_count=200,
            rss_sources=["smaps_rollup:Rss"] * 200,
            ratio_limit="3",
            require_ratio=True,
            expected_image_id="sha256:image",
            cleanup=cleanup,
        )
        self.assertTrue(any("requested backend concurrency" in error for error in errors))
        self.assertIn(
            "Gateway container was OOM-killed",
            memory.validate_full_evidence(
                baseline=100,
                baseline_sample_count=10,
                final_rss=250,
                final_idle_sample_count=10,
                bursts=bursts,
                sample_count=200,
                rss_sources=["smaps_rollup:Rss"] * 200,
                ratio_limit="3",
                require_ratio=True,
                expected_image_id="sha256:image",
                cleanup=dict(cleanup, oom_killed=True),
            ),
        )
        self.assertIn(
            "final idle RSS exceeds the declared ratio limit",
            memory.validate_full_evidence(
                baseline=100,
                baseline_sample_count=10,
                final_rss=301,
                final_idle_sample_count=10,
                bursts=bursts,
                sample_count=200,
                rss_sources=["smaps_rollup:Rss"] * 200,
                ratio_limit="3",
                require_ratio=True,
                expected_image_id="sha256:image",
                cleanup=cleanup,
            ),
        )

    def test_full_evidence_rejects_too_few_bursts(self) -> None:
        burst = {
            "burst_id": "burst-01",
            "requested_streams": memory.FULL_CONCURRENCY,
            "backend_started": memory.FULL_CONCURRENCY,
            "backend_peak_live": memory.FULL_CONCURRENCY,
            "client_peak_live": memory.FULL_CONCURRENCY,
            "backend_finished": memory.FULL_CONCURRENCY,
            "backend_cancelled": 0,
            "invalid_backend_receipts": [],
            "backend_active_after_drain": 0,
            "client_active_after_drain": 0,
            "live_plateau_sample_count": memory.FULL_PLATEAU_SECONDS,
            "settle_sample_count": memory.RSS_SAMPLES,
        }
        errors = memory.validate_full_evidence(
            baseline=100,
            baseline_sample_count=memory.RSS_SAMPLES,
            final_rss=250,
            final_idle_sample_count=memory.RSS_SAMPLES,
            bursts=[dict(burst, burst_id=f"burst-{index + 1:02d}") for index in range(memory.FULL_BURSTS - 1)],
            sample_count=200,
            rss_sources=["smaps_rollup:Rss"] * 200,
            ratio_limit=None,
            require_ratio=False,
            expected_image_id="sha256:image",
            cleanup={
                "container_name": "grid-memory-run-gnu",
                "container_created": True,
                "stop_return_code": 0,
                "post_stop_running": False,
                "container_image_id": "sha256:image",
                "oom_killed": False,
                "exit_code": 0,
                "remove_return_code": 0,
                "post_remove_absent": True,
                "cleanup_errors": [],
                "gateway_pid": 101,
                "gateway_start_ticks": 9876,
                "startup_identity": "starting grid-gateway",
                "cpuset": "0,1,2,3",
                "memory_limit_bytes": memory.MEMORY_LIMIT_BYTES,
            },
        )
        self.assertIn(
            f"full evidence has {memory.FULL_BURSTS - 1} burst records; expected {memory.FULL_BURSTS}",
            errors,
        )

    def test_full_evidence_rejects_wrong_requested_stream_count(self) -> None:
        burst = {
            "burst_id": "burst-01",
            "requested_streams": memory.FULL_CONCURRENCY,
            "backend_started": memory.FULL_CONCURRENCY,
            "backend_peak_live": memory.FULL_CONCURRENCY,
            "client_peak_live": memory.FULL_CONCURRENCY,
            "backend_finished": memory.FULL_CONCURRENCY,
            "backend_cancelled": 0,
            "invalid_backend_receipts": [],
            "backend_active_after_drain": 0,
            "client_active_after_drain": 0,
            "live_plateau_sample_count": memory.FULL_PLATEAU_SECONDS,
            "settle_sample_count": memory.RSS_SAMPLES,
        }
        bursts = [dict(burst, burst_id=f"burst-{index + 1:02d}") for index in range(memory.FULL_BURSTS)]
        bursts[0]["requested_streams"] = memory.FULL_CONCURRENCY - 1
        errors = memory.validate_full_evidence(
            baseline=100,
            baseline_sample_count=memory.RSS_SAMPLES,
            final_rss=250,
            final_idle_sample_count=memory.RSS_SAMPLES,
            bursts=bursts,
            sample_count=200,
            rss_sources=["smaps_rollup:Rss"] * 200,
            ratio_limit=None,
            require_ratio=False,
            expected_image_id="sha256:image",
            cleanup={
                "container_name": "grid-memory-run-gnu",
                "container_created": True,
                "stop_return_code": 0,
                "post_stop_running": False,
                "container_image_id": "sha256:image",
                "oom_killed": False,
                "exit_code": 0,
                "remove_return_code": 0,
                "post_remove_absent": True,
                "cleanup_errors": [],
                "gateway_pid": 101,
                "gateway_start_ticks": 9876,
                "startup_identity": "starting grid-gateway",
                "cpuset": "0,1,2,3",
                "memory_limit_bytes": memory.MEMORY_LIMIT_BYTES,
            },
        )
        self.assertIn(
            f"burst-01 requested {memory.FULL_CONCURRENCY - 1} streams; expected {memory.FULL_CONCURRENCY}",
            errors,
        )


class GatewayCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = mock.patch.object(memory.GatewayContainer, "choose_cpu_set", return_value="0-3")
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_stop(
        self,
        exit_code: int,
        rm_code: int,
        post_remove: subprocess.CompletedProcess[str],
        stop_code: int = 0,
    ) -> dict:
        state = json.dumps([{"Image": "sha256:image", "State": {"Running": False, "ExitCode": exit_code, "OOMKilled": False}}])
        outputs = iter(
            [
                subprocess.CompletedProcess(["docker"], stop_code, "gateway\n", "stop failed" if stop_code else ""),
                subprocess.CompletedProcess(["docker"], 0, state, ""),
                subprocess.CompletedProcess(["docker"], rm_code, "", "remove failed" if rm_code else ""),
                post_remove,
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            container = memory.GatewayContainer(
                "sha256:image", "musl-malloc", "cleanup-test", memory.Path(directory), "127.0.0.1", 9000, {}
            )
            container.container_created = True
            container.container_id = "a" * 64

            def fake_docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
                return next(outputs)

            container.docker = fake_docker
            return container.stop()

    def test_jemalloc_marker_is_read_from_stopped_container_logs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            container = memory.GatewayContainer(
                "sha256:image", "gnu-jemalloc", "exit-log-test", memory.Path(directory), "127.0.0.1", 9000, {}
            )
            container.container_created = True
            container.container_id = "a" * 64
            commands: list[str] = []
            def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
                commands.append(args[0])
                if args[0] == "logs":
                    self.assertEqual(args[:3], ("logs", "--tail", "200"))
                    return subprocess.CompletedProcess(args, 0, "", "opt.background_thread: true\n")
                if args[0] == "inspect":
                    if "rm" in commands:
                        return subprocess.CompletedProcess(args, 1, "", "No such container")
                    return subprocess.CompletedProcess(args, 0, json.dumps([{"State": {"Running": False, "ExitCode": 0}}]), "")
                return subprocess.CompletedProcess(args, 0, "", "")
            container.docker = docker
            result = container.stop()
            self.assertTrue(result["jemalloc_background_thread_verified"])
            self.assertLess(commands.index("logs"), commands.index("rm"))
            self.assertIn("opt.background_thread: true", (memory.Path(directory) / "gateway-exit.log").read_text())

    def test_unexpected_gateway_exit_fails_cleanup(self) -> None:
        absent = subprocess.CompletedProcess(["docker"], 1, "", "Error: No such object: cleanup-test")
        cleanup = self.run_stop(exit_code=23, rm_code=0, post_remove=absent)
        self.assertIn("Gateway exited with unexpected status 23", cleanup["cleanup_errors"])
        self.assertEqual(cleanup["stop_return_code"], 0)
        self.assertEqual(cleanup["remove_return_code"], 0)
        self.assertTrue(cleanup["post_remove_absent"])

    def test_nonzero_docker_stop_fails_cleanup_even_if_removal_succeeds(self) -> None:
        absent = subprocess.CompletedProcess(["docker"], 1, "", "Error: No such object: cleanup-test")
        cleanup = self.run_stop(exit_code=0, rm_code=0, post_remove=absent, stop_code=1)
        self.assertEqual(cleanup["stop_return_code"], 1)
        self.assertIn("Docker stop did not return success", cleanup["cleanup_errors"])

    def test_failed_removal_and_unconfirmed_absence_fail_cleanup(self) -> None:
        still_present = subprocess.CompletedProcess(["docker"], 0, "[]", "")
        failed_remove = self.run_stop(exit_code=0, rm_code=1, post_remove=still_present)
        self.assertIn("Docker container removal did not return success", failed_remove["cleanup_errors"])
        self.assertIn("Gateway container was not confirmed absent after removal", failed_remove["cleanup_errors"])
        self.assertFalse(failed_remove["post_remove_absent"])

        unknown = subprocess.CompletedProcess(["docker"], 1, "", "Cannot connect to the Docker daemon")
        unconfirmed = self.run_stop(exit_code=0, rm_code=0, post_remove=unknown)
        self.assertIsNone(unconfirmed["post_remove_absent"])
        self.assertIn("Gateway container was not confirmed absent after removal", unconfirmed["cleanup_errors"])


class GatewayCreationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        patcher = mock.patch.object(memory.GatewayContainer, "choose_cpu_set", return_value="0-3")
        patcher.start()
        self.addCleanup(patcher.stop)

    async def test_failed_start_removes_only_the_created_container_id(self) -> None:
        await self.check_failed_creation("start", owns_container=True)

    async def test_failed_create_removes_its_partially_created_container(self) -> None:
        await self.check_failed_creation("create", owns_container=True)

    async def test_name_collision_never_removes_the_existing_container(self) -> None:
        await self.check_failed_creation("create", owns_container=False)

    async def check_failed_creation(self, failing_command: str, *, owns_container: bool) -> None:
        with tempfile.TemporaryDirectory() as directory:
            container = memory.GatewayContainer(
                "sha256:image", "musl-malloc", "creation-test", memory.Path(directory), "127.0.0.1", 9000, {}
            )
            container.inspect_image = lambda: None
            commands: list[tuple[str, ...]] = []
            owned_id = "b" * 64

            def fake_docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
                commands.append(args)
                if args[0] == "create" and owns_container:
                    container.cidfile.write_text(owned_id + "\n")
                if args[0] == failing_command:
                    raise memory.MeasurementError("injected Docker startup failure")
                if args[0] == "inspect":
                    if any(command[0] == "rm" for command in commands):
                        return subprocess.CompletedProcess(args, 1, "", "No such container")
                    state = [{"Image": "sha256:image", "State": {"Running": False, "ExitCode": 128, "OOMKilled": False}}]
                    return subprocess.CompletedProcess(args, 0, json.dumps(state), "")
                return subprocess.CompletedProcess(args, 0, owned_id, "")

            container.docker = fake_docker
            with self.assertRaisesRegex(memory.MeasurementError, "injected Docker startup failure"):
                await container.start(memory.Path(directory) / "serving.json", memory.Path(directory) / "praxis.yaml")
            cleanup = container.stop()
            mutations = [args for args in commands if args[0] in ("stop", "rm")]
            if owns_container:
                self.assertEqual(container.container_id, owned_id)
                self.assertEqual(mutations, [("stop", "--time", "15", owned_id), ("rm", "--force", owned_id)])
                self.assertTrue(cleanup["post_remove_absent"])
                self.assertEqual(cleanup["exit_code"], 128)
                self.assertIn("Gateway exited with unexpected status 128", cleanup["cleanup_errors"])
            else:
                self.assertIsNone(container.container_id)
                self.assertEqual(mutations, [])
                self.assertFalse(cleanup["container_created"])
                self.assertEqual([args[0] for args in commands], ["create"])


class DockerArgumentsTests(unittest.TestCase):
    def test_local_log_rotation_allows_the_default_compression(self) -> None:
        options = memory.docker_log_options()
        self.assertEqual(options, ["--log-driver", "local", "--log-opt", "max-size=8m", "--log-opt", "max-file=2"])
        self.assertNotIn("max-file=1", options)

    def test_libc_probe_uses_musl_supported_binary_resolution(self) -> None:
        self.assertEqual(memory.libc_identification_args("gnu-jemalloc"), ("--version",))
        self.assertEqual(memory.libc_identification_args("musl-malloc"), ("/usr/local/bin/grid-gateway",))
        with self.assertRaises(memory.MeasurementError):
            memory.libc_identification_args("unknown")


class GatewayLogCaptureTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = mock.patch.object(memory.GatewayContainer, "choose_cpu_set", return_value="0-3")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_startup_log_is_visible_while_docker_log_stream_is_open(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            container = memory.GatewayContainer(
                "sha256:image", "musl-malloc", "log-test", memory.Path(directory), "127.0.0.1", 9000, {}
            )
            read_fd, write_fd = os.pipe()
            stream = os.fdopen(read_fd, "rb")
            worker = threading.Thread(target=container.copy_logs, args=(stream,))
            worker.start()
            try:
                os.write(write_fd, b"starting grid-gateway test-identity\n")
                deadline = time.monotonic() + 2
                log_path = memory.Path(directory) / "gateway.log"
                while time.monotonic() < deadline:
                    try:
                        if "starting grid-gateway" in log_path.read_text():
                            break
                    except FileNotFoundError:
                        pass
                    time.sleep(0.01)
                self.assertIn("starting grid-gateway", log_path.read_text())
            finally:
                os.close(write_fd)
                worker.join(timeout=2)
                stream.close()
            self.assertFalse(worker.is_alive())


class GatewayConfigTests(unittest.TestCase):
    def test_local_backend_config_allows_private_upstream_addresses(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runner = memory.StreamMemoryRunner.__new__(memory.StreamMemoryRunner)
            runner.evidence = memory.Path(directory)
            runner.backend = memory.argparse.Namespace(port=45678)
            _serving_path, praxis_path = runner.prepare_gateway_config()
            config = praxis_path.read_text()
        self.assertIn("allow_private_endpoints: true", config)
        self.assertIn("allow_private_upstreams: true", config)
        self.assertIn("host.docker.internal:", config)


class HttpParsingTests(unittest.IsolatedAsyncioTestCase):
    async def read_chunked_events(self, payload: bytes) -> list[str | None]:
        reader = asyncio.StreamReader()
        reader.feed_data(chunked(payload))
        reader.feed_eof()
        stream = memory.HttpBody(reader, {"transfer-encoding": "chunked"})
        events: list[str | None] = []
        while True:
            event = await stream.next_event()
            events.append(event)
            if event is None:
                return events

    async def test_sse_parser_streams_multiple_events_from_one_chunk(self) -> None:
        events = await self.read_chunked_events(b"data: first\r\n\r\ndata: [DONE]\n\n")
        self.assertEqual(events, ["first", "[DONE]", None])

    async def test_generated_openai_event_matches_the_declared_slow_reader_size(self) -> None:
        empty = memory.openai_sse_delta("")
        content = "x" * (memory.SLOW_READER_EVENT_BYTES - len(empty))
        payload = memory.openai_sse_delta(content)
        self.assertEqual(len(payload), memory.SLOW_READER_EVENT_BYTES)
        events = await self.read_chunked_events(payload)
        self.assertEqual(len(events[0] or ""), len(payload.decode().removeprefix("data: ").strip()))
        memory.StreamMemoryRunner.validate_sse_payload(events[0], "request")
        with self.assertRaises(memory.MeasurementError):
            memory.StreamMemoryRunner.validate_sse_payload("not-json", "request")

    async def test_sse_parser_ignores_comments_without_buffering_prior_events(self) -> None:
        events = await self.read_chunked_events(b": heartbeat\n\ndata: value\n\n")
        self.assertEqual(events, ["value", None])

    async def test_chunk_size_is_bounded(self) -> None:
        reader = asyncio.StreamReader()
        body = b"a" * (memory.MAX_SSE_EVENT_BYTES + 1)
        reader.feed_data(f"{len(body):x}\r\n".encode())
        stream = memory.HttpBody(reader, {"transfer-encoding": "chunked"})
        with self.assertRaises(memory.MeasurementError):
            await stream.next_event()

    async def test_http_head_rejects_incomplete_and_malformed_data(self) -> None:
        with self.assertRaises(memory.MeasurementError):
            memory.parse_http_head(b"HTTP/1.1 200 OK\r\n")
        with self.assertRaises(memory.MeasurementError):
            memory.parse_http_head(b"not HTTP\r\n\r\n")


class BackendBarrierTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.backend = memory.Backend("run-test")
        self.state = memory.BurstState("burst-01", {"request-01"})
        self.backend.states[self.state.burst_id] = self.state
        await self.backend.start()

    async def asyncTearDown(self) -> None:
        await self.backend.close()

    async def open_stream(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, memory.HttpBody]:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.backend.port)
        body = b'{"model":"llama","stream":true}'
        request = (
            "POST /v1/chat/completions HTTP/1.1\r\n"
            "Host: localhost\r\n"
            f"Content-Length: {len(body)}\r\n"
            "X-Qualification-Run: run-test\r\n"
            "X-Qualification-Burst: burst-01\r\n"
            "X-Qualification-Request: request-01\r\n"
            "X-Qualification-Phase: stream\r\n\r\n"
        ).encode() + body
        writer.write(request)
        await writer.drain()
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=2)
        except BaseException:
            writer.close()
            await writer.wait_closed()
            raise
        status, headers = memory.parse_http_head(head)
        if status != 200:
            raise AssertionError(f"backend returned HTTP {status}")
        stream = memory.HttpBody(reader, headers)
        self.assertTrue((await asyncio.wait_for(stream.next_event(), 2) or "").startswith("{"))
        return reader, writer, stream

    async def test_duplicate_handler_does_not_cancel_the_original_receipt(self) -> None:
        _reader, writer, _body = await self.open_stream()
        try:
            with self.assertRaises(asyncio.IncompleteReadError):
                await self.open_stream()
            self.assertEqual(self.state.active, {"request-01"})
            self.assertEqual(self.state.cancelled, set())
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_release_completes_one_barriered_stream_and_counts_it(self) -> None:
        _reader, writer, stream = await self.open_stream()
        await self.state.wait_for(lambda: len(self.state.active) == 1, 2, "backend did not register stream")
        self.assertEqual(self.state.peak_active, 1)
        self.state.release.set()
        self.assertEqual(await asyncio.wait_for(stream.next_event(), 2), "[DONE]")
        self.assertIsNone(await asyncio.wait_for(stream.next_event(), 2))
        writer.close()
        await writer.wait_closed()
        await self.state.wait_for(lambda: not self.state.active, 2, "backend stream did not drain")
        self.assertEqual(self.state.finished, {"request-01"})
        self.assertEqual(self.state.cancelled, set())
        self.assertEqual(self.backend.errors, [])

    async def test_client_disconnect_is_counted_as_cancellation(self) -> None:
        _reader, writer, _stream = await self.open_stream()
        writer.close()
        await writer.wait_closed()
        await self.state.wait_for(lambda: len(self.state.cancelled) == 1, 2, "backend did not observe disconnect")
        self.assertEqual(self.state.active, set())
        self.assertEqual(self.backend.errors, [])


class SlowReaderProfileTests(unittest.IsolatedAsyncioTestCase):
    async def test_profile_records_backend_concurrency_before_continuation(self) -> None:
        class State:
            def __init__(self, count: int) -> None:
                self.active = {f"request-{index}" for index in range(count)}
                self.finished: set[str] = set()
                self.cancelled: set[str] = set()
                self.continued = False
                self.slow_start = mock.Mock()
                self.slow_start.set.side_effect = self.continue_stream

            def continue_stream(self) -> None:
                self.continued = True

            def release(self) -> None:
                self.finished.update(self.active)
                self.active.clear()

            async def wait_for(self, predicate, _timeout, message: str) -> None:
                if not predicate() and self.continued:
                    self.release()
                if not predicate():
                    raise AssertionError(message)

        concurrency = 4
        state = State(concurrency)
        runner = memory.StreamMemoryRunner.__new__(memory.StreamMemoryRunner)
        runner.start_burst = mock.AsyncMock(return_value=state)
        runner.sampler = mock.Mock()
        runner.client_active = {f"request-{index}" for index in range(concurrency)}
        runner.request_tasks = {"slow-reader-01": []}
        runner.client_peak_sse_buffer_bytes = 0
        runner.summary = {}

        with (
            mock.patch.object(memory, "SLOW_READER_CONCURRENCY", concurrency),
            mock.patch.object(memory, "SLOW_READER_PAUSE_SECONDS", 0),
        ):
            await runner.run_slow_reader_profile()

        self.assertEqual(runner.summary["slow_reader"]["backend_active_before_continuation"], concurrency)
        self.assertEqual(runner.summary["slow_reader"]["backend_active_after_drain"], 0)


class WarmupBarrierTests(unittest.IsolatedAsyncioTestCase):
    async def test_warmup_uses_the_shared_release_barrier(self) -> None:
        backend = memory.Backend("warmup-run")
        await backend.start()
        runner = memory.StreamMemoryRunner.__new__(memory.StreamMemoryRunner)
        runner.args = memory.argparse.Namespace(mode="quick")
        runner.run_id = "warmup-run"
        runner.backend = backend
        runner.gateway = memory.argparse.Namespace(host_port=backend.port)
        runner.sampler = None
        runner.client_active = backend.client_active
        runner.request_tasks = {}
        runner.summary = {}
        runner.client_peak_sse_buffer_bytes = 0
        try:
            await asyncio.wait_for(runner.warm_gateway(), timeout=5)
            self.assertEqual(runner.summary["warmup"]["requested_streams"], 4)
            self.assertEqual(runner.summary["warmup"]["backend_started"], 4)
            self.assertEqual(runner.summary["warmup"]["backend_finished"], 4)
            self.assertEqual(runner.summary["warmup"]["backend_cancelled"], 0)
            self.assertEqual(runner.summary["warmup"]["backend_active_after_drain"], 0)
            self.assertEqual(runner.summary["warmup"]["client_active_after_drain"], 0)
            self.assertEqual(backend.errors, [])
        finally:
            await runner.release_requests()
            await backend.close()


class ObserveModeTests(unittest.IsolatedAsyncioTestCase):
    async def test_observe_requires_complete_full_evidence_without_claiming_qualification(self) -> None:
        runner = memory.StreamMemoryRunner.__new__(memory.StreamMemoryRunner)
        runner.args = memory.argparse.Namespace(mode="observe", variant="musl-malloc", max_settle_ratio=None)
        runner.run_id = "observe-test"
        runner.started_at = "2026-11-20T00:00:00+00:00"
        runner.source = {"commit": "a" * 40, "fingerprint_sha256": "fingerprint"}
        runner.summary = {
            "warm_idle_baseline_sample_count": memory.RSS_SAMPLES,
            "final_idle_samples": memory.RSS_SAMPLES,
            "final_idle_rss_median_bytes": 250,
        }
        runner.failures = []
        runner.baseline = 100
        runner.bursts = [
            {
                "burst_id": f"burst-{index + 1:02d}",
                "requested_streams": memory.FULL_CONCURRENCY,
                "backend_started": memory.FULL_CONCURRENCY,
                "backend_peak_live": memory.FULL_CONCURRENCY,
                "client_peak_live": memory.FULL_CONCURRENCY,
                "backend_finished": memory.FULL_CONCURRENCY,
                "backend_cancelled": 0,
                "invalid_backend_receipts": [],
                "backend_active_after_drain": 0,
                "client_active_after_drain": 0,
                "live_plateau_sample_count": memory.FULL_PLATEAU_SECONDS,
                "settle_sample_count": memory.RSS_SAMPLES,
            }
            for index in range(memory.FULL_BURSTS)
        ]
        runner.sampler = memory.argparse.Namespace(samples=[{"rss_source": "smaps_rollup:Rss"}] * 200, errors=[])
        runner.backend = memory.argparse.Namespace(errors=[], client_active=set())
        runner.client_active = runner.backend.client_active
        runner.gateway = memory.argparse.Namespace(image_data={"id": "sha256:image"})
        runner.container_cleanup = {
            "container_name": "grid-memory-observe",
            "container_created": True,
            "stop_return_code": 0,
            "post_stop_running": False,
            "container_image_id": "sha256:image",
            "oom_killed": False,
            "exit_code": 0,
            "remove_return_code": 0,
            "post_remove_absent": True,
            "cleanup_errors": [],
            "gateway_pid": 101,
            "gateway_start_ticks": 9876,
            "startup_identity": "starting grid-gateway",
            "cpuset": "0,1,2,3",
            "memory_limit_bytes": memory.MEMORY_LIMIT_BYTES,
        }
        runner._sigterm_received = False
        output: dict[str, object] = {}
        runner.write_json = lambda name, value: output.__setitem__(name, value)

        await runner.write_final_summary()

        summary = output["summary.json"]
        self.assertEqual(summary["outcome"], "OBSERVATION_COMPLETE_NOT_QUALIFICATION")
        self.assertTrue(summary["observation_complete"])
        self.assertFalse(summary["qualification_pass"])
        self.assertFalse(summary["qualification_claim"])
        self.assertEqual(summary["failures"], [])


class SignalCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_sigterm_cancels_once_and_runs_backend_and_gateway_cleanup(self) -> None:
        await self.check_cleanup(sampler_close_raises=False)

    async def test_sampler_close_failure_still_stops_and_removes_gateway(self) -> None:
        await self.check_cleanup(sampler_close_raises=True)

    async def check_cleanup(self, sampler_close_raises: bool) -> None:
        events: list[str] = []

        class FakeBackend:
            def __init__(self) -> None:
                self.states: dict[str, object] = {}
                self.errors: list[str] = []
                self.client_active: set[str] = set()
                self.port = 45678

            async def start(self, host: str) -> None:
                self.host = host
                events.append("backend.start")

            async def close(self) -> None:
                events.append("backend.close")

        class FakeGateway:
            def __init__(self, *_args: object) -> None:
                self.image_data = {"id": "sha256:image"}

            async def start(self, _serving: object, _praxis: object) -> None:
                events.append("gateway.start")

            def stop(self) -> dict[str, object]:
                events.append("gateway.stop_and_remove")
                return {
                    "container_name": "grid-memory-sigterm",
                    "container_created": True,
                    "stop_return_code": 0,
                    "post_stop_running": False,
                    "container_image_id": "sha256:image",
                    "oom_killed": False,
                    "exit_code": 0,
                    "remove_return_code": 0,
                    "post_remove_absent": True,
                    "cleanup_errors": [],
                    "gateway_pid": 101,
                    "gateway_start_ticks": 9876,
                    "startup_identity": "starting grid-gateway",
                    "cpuset": "0,1,2,3",
                    "memory_limit_bytes": memory.MEMORY_LIMIT_BYTES,
                }

        class FakeSampler:
            def __init__(self, *_args: object) -> None:
                self.samples: list[dict[str, object]] = []
                self.errors: list[str] = []

            async def start(self) -> None:
                events.append("sampler.start")

            async def close(self) -> None:
                events.append("sampler.close")
                if sampler_close_raises:
                    raise OSError("evidence disk full")

        with tempfile.TemporaryDirectory() as directory:
            runner = memory.StreamMemoryRunner.__new__(memory.StreamMemoryRunner)
            source_revision = "a" * 40
            runner.args = memory.argparse.Namespace(
                source_revision=source_revision,
                mode="observe",
                variant="musl-malloc",
                image="sha256:image",
                max_settle_ratio=None,
            )
            runner.repo = memory.Path(directory)
            runner.source = {"commit": source_revision, "fingerprint_sha256": "fingerprint"}
            runner.run_id = "sigterm-test"
            runner.evidence = memory.Path(directory)
            runner.backend = FakeBackend()
            runner.gateway = None
            runner.sampler = None
            runner.client_active = runner.backend.client_active
            runner.request_tasks = {}
            runner.bursts = []
            runner.failures = []
            runner.container_cleanup = None
            runner.baseline = None
            runner.client_peak_sse_buffer_bytes = 0
            runner.summary = {}
            runner.started_at = "2026-11-20T00:00:00+00:00"
            runner.host = {"machine": "x86_64", "docker_host_gateway": "172.17.0.1"}
            runner._main_task = None
            runner._sigterm_received = False
            runner._sigterm_cancel_requested = False
            runner._cleanup_started = False
            runner.validate_host = lambda: None
            runner.prepare_gateway_config = lambda: ("serving", "praxis")

            async def send_sigterm() -> None:
                events.append("signal.SIGTERM")
                os.kill(os.getpid(), signal.SIGTERM)
                await asyncio.sleep(30)

            runner.run_memory_profile = send_sigterm

            with mock.patch.object(memory, "GatewayContainer", FakeGateway), mock.patch.object(
                memory, "Sampler", FakeSampler
            ):
                result = await runner.run()

        self.assertEqual(result, 2)
        self.assertEqual(
            events,
            [
                "backend.start",
                "gateway.start",
                "sampler.start",
                "signal.SIGTERM",
                "backend.close",
                "sampler.close",
                "gateway.stop_and_remove",
            ],
        )
        self.assertIn("interrupted by SIGTERM", runner.summary["failures"])
        self.assertTrue(runner.summary["sigterm_received"])
        self.assertFalse(runner.summary["qualification_pass"])


class ArgumentTests(unittest.TestCase):
    def test_full_mode_requires_an_explicit_ratio(self) -> None:
        error_output = io.StringIO()
        with contextlib.redirect_stderr(error_output):
            with self.assertRaises(SystemExit) as error:
                memory.parse_args(
                    [
                        "--mode",
                        "full",
                        "--variant",
                        "gnu-jemalloc",
                        "--image",
                        "sha256:" + "a" * 64,
                        "--source-revision",
                        "a" * 40,
                        "--evidence-dir",
                        "/tmp/evidence",
                    ]
                )
        self.assertEqual(error.exception.code, 2)
        self.assertIn("--max-settle-ratio is required", error_output.getvalue())

    def test_observe_mode_rejects_a_ratio_argument(self) -> None:
        error_output = io.StringIO()
        with contextlib.redirect_stderr(error_output):
            with self.assertRaises(SystemExit) as error:
                memory.parse_args(
                    [
                        "--mode",
                        "observe",
                        "--variant",
                        "gnu-jemalloc",
                        "--image",
                        "sha256:" + "a" * 64,
                        "--source-revision",
                        "a" * 40,
                        "--max-settle-ratio",
                        "3",
                        "--evidence-dir",
                        "/tmp/evidence",
                    ]
                )
        self.assertEqual(error.exception.code, 2)
        self.assertIn("only accepted with --mode full", error_output.getvalue())


if __name__ == "__main__":
    unittest.main()
