#!/usr/bin/env python3
"""Measure Grid Gateway process RSS while controlled SSE streams open and drain."""

from __future__ import annotations

import argparse
import asyncio
import collections
import dataclasses
import datetime as dt
import decimal
import hashlib
import ipaddress
import json
import os
import platform
import re
import resource
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid
from pathlib import Path
from typing import Any


SAMPLE_PERIOD_SECONDS = 1.0
RSS_SAMPLES = 10
MAX_HTTP_HEAD_BYTES = 64 * 1024
MAX_SSE_EVENT_BYTES = 64 * 1024
MAX_GATEWAY_LOG_BYTES = 4 * 1024 * 1024
MAX_GATEWAY_LOG_TAIL_BYTES = 1024 * 1024
MEMORY_LIMIT_BYTES = 4 * 1024**3
MEMORY_LIMIT_TEXT = "4g"
FULL_HOST_MEMORY_AVAILABLE_BYTES = 8 * 1024**3
FULL_DISK_FREE_BYTES = 10 * 1024**3
FULL_CONCURRENCY = 1024
FULL_BURSTS = 8
FULL_PLATEAU_SECONDS = 5
FULL_BURST_SETTLE_SECONDS = 10
FULL_FINAL_IDLE_SECONDS = 120
FULL_WARMUP_CONCURRENCY = 16
FULL_BASELINE_SECONDS = 10
FULL_WARM_SETTLE_SECONDS = 20
SLOW_READER_CONCURRENCY = 4
SLOW_READER_EVENTS = 64
SLOW_READER_EVENT_BYTES = 16 * 1024
SLOW_READER_EVENT_INTERVAL_SECONDS = 0.1
SLOW_READER_PAUSE_SECONDS = 3.0


class MeasurementError(RuntimeError):
    """A qualification run cannot support its stated result."""


def utc_now() -> str:
    """Return the current time in a stable UTC representation."""
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def parse_proc_start_ticks(stat_text: str) -> int:
    """Read Linux proc stat field 22 while allowing spaces and ')' in comm."""
    closing = stat_text.rfind(")")
    if closing < 0:
        raise ValueError("proc stat has no closing comm delimiter")
    fields = stat_text[closing + 1 :].split()
    if len(fields) <= 19:
        raise ValueError("proc stat is missing start-time field 22")
    return int(fields[19])


def validate_process_start_ticks(actual: int, expected: int) -> None:
    """Reject a missing or reused process identity before accepting an RSS sample."""
    if expected <= 0 or actual != expected:
        raise MeasurementError("Gateway PID was reused or its start identity is invalid")


def validate_image_labels(labels: dict[str, str], source: dict[str, Any], variant: str) -> None:
    """Require image provenance to match the exact source tree and runtime variant."""
    if labels.get("org.opencontainers.image.revision") != source["commit"]:
        raise MeasurementError("image revision label does not match checkout HEAD")
    expected_allocator = "jemalloc" if variant == "gnu-jemalloc" else "system"
    if labels.get("org.grid.qualification.memory.variant") != variant:
        raise MeasurementError("image memory-variant label does not match the requested variant")
    if labels.get("org.grid.qualification.memory.allocator") != expected_allocator:
        raise MeasurementError("image allocator label does not match the requested variant")
    if labels.get("org.grid.qualification.source-sha256") != source["fingerprint_sha256"]:
        raise MeasurementError("image source fingerprint label does not match the current checkout")


def validate_immutable_image_reference(reference: str, image_id: str | None, digests: list[str]) -> None:
    """Reject mutable image tags so the measured container cannot drift after inspection."""
    if image_id is None or reference not in {image_id, *digests}:
        raise MeasurementError("--image must be a Docker image ID or repository digest, not a mutable tag")


def libc_identification_args(variant: str) -> tuple[str, ...]:
    """Choose a libc probe supported by the runtime image."""
    if variant == "gnu-jemalloc":
        return ("--version",)
    if variant == "musl-malloc":
        return ("/usr/local/bin/grid-gateway",)
    raise MeasurementError(f"unknown Gateway memory variant: {variant}")


def is_safe_immutable_image_reference(reference: str) -> bool:
    """Accept only local IDs or repository digests, avoiding credentials in evidence."""
    return re.fullmatch(r"(?:[A-Za-z0-9._/:+-]+@)?sha256:[0-9a-f]{64}", reference) is not None


def validate_file_descriptor_limit(soft_limit: int, required: int = 4096) -> None:
    """Require enough host descriptors, while accepting an unlimited soft limit."""
    if soft_limit != resource.RLIM_INFINITY and soft_limit < required:
        raise MeasurementError(f"file descriptor limit {soft_limit} is below the required {required}")


def measurement_user(uid: int, gid: int) -> str:
    """Run the nonroot measurement process as the host caller so /proc is readable."""
    if uid <= 0 or gid < 0:
        raise MeasurementError("the process RSS sampler requires a nonroot host UID and valid GID")
    return f"{uid}:{gid}"


def validate_gateway_user(status: str, expected_uid: int, expected_gid: int) -> None:
    """Require the Gateway to retain the nonroot host UID/GID used for host /proc access."""
    fields = {line.partition(":")[0]: line.partition(":")[2].split() for line in status.splitlines()}
    try:
        uids = [int(value) for value in fields["Uid"]]
        gids = [int(value) for value in fields["Gid"]]
    except (KeyError, ValueError) as error:
        raise MeasurementError("Gateway process UID/GID identity is absent or invalid") from error
    if len(uids) < 2 or len(gids) < 2 or uids[1] != expected_uid or gids[1] != expected_gid or uids[1] == 0:
        raise MeasurementError("Gateway process did not retain the configured nonroot UID/GID")


def gateway_cleanup_errors(cleanup: dict[str, Any]) -> list[str]:
    """Require a clean Gateway exit and verified container removal in every mode."""
    errors: list[str] = []
    if cleanup.get("container_created") is not True:
        errors.append("Gateway container was not created")
        return errors
    if cleanup.get("stop_return_code") != 0:
        errors.append("Docker stop did not return success")
    if cleanup.get("post_stop_running") is not False:
        errors.append("Gateway container was not confirmed stopped")
    if cleanup.get("exit_code") is None:
        errors.append("Gateway container exit code could not be verified")
    elif cleanup.get("exit_code") != 0:
        errors.append(f"Gateway exited with unexpected status {cleanup['exit_code']}")
    if cleanup.get("remove_return_code") != 0:
        errors.append("Docker container removal did not return success")
    if cleanup.get("post_remove_absent") is not True:
        errors.append("Gateway container was not confirmed absent after removal")
    return errors


def docker_log_options() -> list[str]:
    """Return bounded local-driver options compatible with log compression."""
    return ["--log-driver", "local", "--log-opt", "max-size=8m", "--log-opt", "max-file=2"]


def parse_cpu_set(value: str) -> set[int]:
    """Parse Linux's comma-separated CPU IDs and inclusive ranges."""
    cpus: set[int] = set()
    if not value:
        raise ValueError("CPU set is empty")
    for token in value.split(","):
        first, separator, last = token.partition("-")
        if not first.isdecimal() or (separator and not last.isdecimal()):
            raise ValueError("CPU set contains an invalid ID or range")
        start = int(first)
        end = int(last) if separator else start
        if end < start:
            raise ValueError("CPU range ends before it starts")
        cpus.update(range(start, end + 1))
    return cpus


def parse_rss_kib(text: str, key: str) -> int:
    """Parse a named Linux proc memory field expressed in KiB."""
    for line in text.splitlines():
        name, separator, value = line.partition(":")
        if separator and name.strip() == key:
            tokens = value.split()
            if len(tokens) < 2 or tokens[1] != "kB":
                raise ValueError(f"{key} is not expressed in kB")
            kib = int(tokens[0])
            if kib < 0:
                raise ValueError(f"{key} cannot be negative")
            return kib
    raise ValueError(f"{key} is absent")


def parse_process_rss(smaps_rollup: str | None, status: str) -> tuple[int, str]:
    """Return process RSS in bytes, preferring smaps_rollup over VmRSS."""
    if smaps_rollup is not None:
        try:
            return parse_rss_kib(smaps_rollup, "Rss") * 1024, "smaps_rollup:Rss"
        except (ValueError, OSError):
            pass
    return parse_rss_kib(status, "VmRSS") * 1024, "status:VmRSS"


def ratio_value(baseline_bytes: int, final_bytes: int) -> decimal.Decimal:
    """Return final-to-baseline RSS ratio without fixed-width arithmetic."""
    if baseline_bytes <= 0:
        raise ValueError("baseline RSS must be positive")
    if final_bytes < 0:
        raise ValueError("final RSS cannot be negative")
    return decimal.Decimal(final_bytes) / decimal.Decimal(baseline_bytes)


def ratio_exceeded(baseline_bytes: int, final_bytes: int, limit: str) -> bool:
    """Compare RSS using decimal arithmetic so large byte counts stay exact."""
    if baseline_bytes <= 0:
        raise ValueError("baseline RSS must be positive")
    if final_bytes < 0:
        raise ValueError("final RSS cannot be negative")
    try:
        maximum = decimal.Decimal(limit)
    except decimal.InvalidOperation as error:
        raise ValueError("ratio limit is not a decimal number") from error
    if not maximum.is_finite() or maximum <= 0:
        raise ValueError("ratio limit must be finite and positive")
    return decimal.Decimal(final_bytes) > decimal.Decimal(baseline_bytes) * maximum


def median_bytes(values: list[int]) -> int:
    """Return an integer median, rejecting an empty measurement window."""
    if not values:
        raise ValueError("measurement window has no RSS samples")
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    lower, upper = ordered[middle - 1], ordered[middle]
    return lower // 2 + upper // 2 + (lower % 2 + upper % 2) // 2


def validate_full_evidence(
    *,
    baseline: int | None,
    baseline_sample_count: int,
    final_rss: int | None,
    final_idle_sample_count: int,
    bursts: list[dict[str, Any]],
    sample_count: int,
    ratio_limit: str | None,
    require_ratio: bool,
    expected_image_id: str | None,
    cleanup: dict[str, Any] | None,
    rss_sources: list[str | None],
) -> list[str]:
    """Return evidence-contract violations that must prevent a full qualification pass."""
    errors: list[str] = []
    if baseline is None or baseline <= 0:
        errors.append("full evidence has no positive warm idle baseline")
    if baseline_sample_count < RSS_SAMPLES:
        errors.append(f"warm baseline has only {baseline_sample_count} RSS samples")
    if final_rss is None or final_rss <= 0:
        errors.append("full evidence has no positive final idle RSS")
    if final_idle_sample_count < RSS_SAMPLES:
        errors.append(f"final idle window has only {final_idle_sample_count} RSS samples")
    if sample_count < RSS_SAMPLES:
        errors.append(f"full evidence has only {sample_count} RSS samples")
    if len(rss_sources) != sample_count:
        errors.append("full evidence RSS source count does not match the recorded sample count")
    supported_sources = {"smaps_rollup:Rss", "status:VmRSS"}
    if any(source not in supported_sources for source in rss_sources):
        errors.append("full evidence contains a missing or unsupported RSS measurement source")
    if len(set(rss_sources)) > 1:
        errors.append("full evidence changes RSS measurement source during the run")
    if len(bursts) != FULL_BURSTS:
        errors.append(f"full evidence has {len(bursts)} burst records; expected {FULL_BURSTS}")
    for burst in bursts:
        requested = burst.get("requested_streams")
        if type(requested) is not int or requested <= 0:
            errors.append("burst evidence has an invalid requested stream count")
            continue
        if requested != FULL_CONCURRENCY:
            errors.append(
                f"{burst.get('burst_id', 'burst')} requested {requested} streams; expected {FULL_CONCURRENCY}"
            )
        if burst.get("backend_started") != requested:
            errors.append(f"{burst.get('burst_id', 'burst')} backend-start count does not match")
        if burst.get("backend_peak_live") != requested:
            errors.append(f"{burst.get('burst_id', 'burst')} never reached requested backend concurrency")
        if burst.get("client_peak_live") != requested:
            errors.append(f"{burst.get('burst_id', 'burst')} never reached requested client concurrency")
        if burst.get("live_plateau_sample_count", 0) < FULL_PLATEAU_SECONDS:
            errors.append(f"{burst.get('burst_id', 'burst')} lacks live plateau RSS samples")
        if burst.get("settle_sample_count", 0) < RSS_SAMPLES:
            errors.append(f"{burst.get('burst_id', 'burst')} lacks settled RSS samples")
        if burst.get("backend_finished") != requested or burst.get("backend_cancelled") != 0:
            errors.append(f"{burst.get('burst_id', 'burst')} did not complete every stream successfully")
        if burst.get("invalid_backend_receipts"):
            errors.append(f"{burst.get('burst_id', 'burst')} contains invalid backend receipts")
        if burst.get("backend_active_after_drain") != 0 or burst.get("client_active_after_drain") != 0:
            errors.append(f"{burst.get('burst_id', 'burst')} did not prove zero active streams after drain")
    if cleanup is None:
        errors.append("full evidence is missing Gateway container cleanup state")
    else:
        errors.extend(gateway_cleanup_errors(cleanup))
        if cleanup.get("oom_killed") is True:
            errors.append("Gateway container was OOM-killed")
        if cleanup.get("oom_killed") is not False or cleanup.get("exit_code") is None:
            errors.append("Gateway container exit state is incomplete")
        if cleanup.get("exit_code") == 137:
            errors.append("Gateway required SIGKILL after the stop timeout")
        if cleanup.get("cleanup_errors"):
            errors.append("Gateway cleanup reported an error")
        if not cleanup.get("container_name"):
            errors.append("Gateway container identity is missing")
        if expected_image_id is None or cleanup.get("container_image_id") != expected_image_id:
            errors.append("Gateway container image ID does not match the locked image")
        if type(cleanup.get("gateway_pid")) is not int or cleanup["gateway_pid"] <= 0:
            errors.append("Gateway process ID is missing")
        if type(cleanup.get("gateway_start_ticks")) is not int or cleanup["gateway_start_ticks"] <= 0:
            errors.append("Gateway process start identity is missing")
        if not cleanup.get("startup_identity") or not cleanup.get("cpuset"):
            errors.append("Gateway process startup or CPU-set identity is missing")
        if cleanup.get("memory_limit_bytes") != MEMORY_LIMIT_BYTES:
            errors.append("Gateway memory limit does not match the fixed profile")
    if require_ratio and baseline is not None and baseline > 0 and final_rss is not None and final_rss > 0:
        if ratio_limit is None:
            errors.append("full evidence is missing the final-to-baseline ratio limit")
        else:
            try:
                if ratio_exceeded(baseline, final_rss, ratio_limit):
                    errors.append("final idle RSS exceeds the declared ratio limit")
            except (ValueError, decimal.InvalidOperation):
                errors.append("full evidence has an invalid final-to-baseline ratio limit")
    return errors


def git_identity(repo: Path) -> dict[str, Any]:
    """Fingerprint tracked changes and untracked source files for this build tree."""
    def git(*args: str) -> bytes:
        result = subprocess.run(
            ["git", *args],
            cwd=repo,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return result.stdout

    head = git("rev-parse", "HEAD").decode().strip()
    tree = git("rev-parse", "HEAD^{tree}").decode().strip()
    diff = git("diff", "--binary", "HEAD", "--").replace(b"\r\n", b"\n")
    untracked = git("ls-files", "--others", "--exclude-standard", "-z").split(b"\0")
    digest = hashlib.sha256()
    included: list[str] = []
    for raw_path in sorted(path for path in untracked if path):
        path = repo / os.fsdecode(raw_path)
        try:
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                content = os.fsencode(os.readlink(path))
            elif stat.S_ISREG(mode):
                content = path.read_bytes()
            else:
                continue
        except OSError as error:
            raise MeasurementError(f"could not fingerprint untracked source file {os.fsdecode(raw_path)}") from error
        digest.update(raw_path)
        digest.update(b"\0")
        digest.update(str(stat.S_IFMT(mode)).encode())
        digest.update(b":")
        digest.update(str(stat.S_IMODE(mode)).encode())
        digest.update(b"\0")
        digest.update(hashlib.sha256(content).digest())
        included.append(os.fsdecode(raw_path))
    status = git("status", "--porcelain=v1", "--untracked-files=all").decode()
    try:
        branch = git("symbolic-ref", "--short", "HEAD").decode().strip()
    except subprocess.CalledProcessError:
        branch = "DETACHED"
    remote_lines = git("remote", "-v").decode().splitlines()
    remotes: dict[str, dict[str, str]] = {}
    for line in remote_lines:
        fields = line.split()
        if len(fields) < 3:
            continue
        name, raw_url, direction = fields[:3]
        if "://" in raw_url:
            parsed = urllib.parse.urlsplit(raw_url)
            hostname = parsed.hostname or ""
            if parsed.port is not None:
                hostname = f"{hostname}:{parsed.port}"
            safe_url = urllib.parse.urlunsplit((parsed.scheme, hostname, parsed.path, "", ""))
        elif "@" in raw_url and ":" in raw_url:
            safe_url = raw_url.split("@", 1)[1]
        else:
            safe_url = raw_url
        remotes.setdefault(name, {})[direction.strip("()")] = safe_url
    fingerprint_material = json.dumps(
        {
            "commit": head,
            "commit_tree": tree,
            "tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
            "untracked_content_sha256": digest.hexdigest(),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return {
        "commit": head,
        "commit_tree": tree,
        "branch": branch,
        "remotes": remotes,
        "tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
        "untracked_content_sha256": digest.hexdigest(),
        "fingerprint_sha256": hashlib.sha256(fingerprint_material).hexdigest(),
        "untracked_files": included,
        "dirty": bool(status),
        "status_lines": status.splitlines(),
    }


def parse_http_head(raw: bytes) -> tuple[int, dict[str, str]]:
    """Parse an HTTP/1 response head with normalized header names."""
    if len(raw) > MAX_HTTP_HEAD_BYTES or not raw.endswith(b"\r\n\r\n"):
        raise MeasurementError("HTTP response head is oversized or incomplete")
    lines = raw[:-4].split(b"\r\n")
    first = lines[0].decode("latin-1") if lines else ""
    pieces = first.split()
    if len(pieces) < 2 or not pieces[0].startswith("HTTP/"):
        raise MeasurementError("Gateway returned an invalid HTTP status line")
    try:
        status = int(pieces[1])
    except ValueError as error:
        raise MeasurementError("Gateway returned a nonnumeric HTTP status") from error
    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, separator, value = line.partition(b":")
        if not separator:
            raise MeasurementError("Gateway returned a malformed HTTP header")
        headers[name.decode("latin-1").strip().lower()] = value.decode("latin-1").strip()
    return status, headers


def openai_sse_delta(content: str) -> bytes:
    """Build one bounded OpenAI-compatible chat-completion SSE data event."""
    value = {
        "id": "chatcmpl-stream-memory",
        "object": "chat.completion.chunk",
        "model": "llama",
        "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}],
    }
    return b"data: " + json.dumps(value, separators=(",", ":")).encode() + b"\n\n"


class HttpBody:
    """Incrementally decode HTTP body framing and bounded SSE events."""

    def __init__(self, reader: asyncio.StreamReader, headers: dict[str, str]) -> None:
        self.reader = reader
        self.chunked = "chunked" in headers.get("transfer-encoding", "").lower()
        self.remaining = int(headers["content-length"]) if "content-length" in headers else None
        self.buffer = bytearray()
        self.peak_buffer_bytes = 0
        self.finished = False

    async def _read_chunk(self) -> bytes:
        if self.finished:
            return b""
        if self.chunked:
            line = await self.reader.readline()
            if not line or len(line) > 128:
                raise MeasurementError("invalid HTTP chunk size line")
            size_text = line.split(b";", 1)[0].strip()
            if not re.fullmatch(rb"[0-9A-Fa-f]+", size_text):
                raise MeasurementError("invalid HTTP chunk size")
            size = int(size_text, 16)
            if size < 0 or size > MAX_SSE_EVENT_BYTES:
                raise MeasurementError("HTTP chunk exceeds the bounded event size")
            if size == 0:
                trailer_bytes = 0
                while True:
                    trailer = await self.reader.readline()
                    trailer_bytes += len(trailer)
                    if trailer in (b"\r\n", b"\n", b""):
                        break
                    if trailer_bytes > MAX_HTTP_HEAD_BYTES:
                        raise MeasurementError("HTTP trailer is oversized")
                self.finished = True
                return b""
            body = await self.reader.readexactly(size)
            ending = await self.reader.readexactly(2)
            if ending != b"\r\n":
                raise MeasurementError("HTTP chunk has an invalid terminator")
            return body
        if self.remaining is not None:
            if self.remaining == 0:
                self.finished = True
                return b""
            size = min(16 * 1024, self.remaining)
            body = await self.reader.readexactly(size)
            self.remaining -= len(body)
            return body
        body = await self.reader.read(16 * 1024)
        if not body:
            self.finished = True
        return body

    async def next_event(self) -> str | None:
        """Return one SSE event's data field without storing previous events."""
        while True:
            normalized = bytes(self.buffer).replace(b"\r\n", b"\n")
            boundary = normalized.find(b"\n\n")
            if boundary >= 0:
                event = normalized[:boundary]
                remainder = normalized[boundary + 2 :]
                self.buffer = bytearray(remainder)
                for line in event.splitlines():
                    if line.startswith(b"data:"):
                        return line[5:].lstrip().decode("utf-8", "replace")
            if self.finished:
                if self.buffer:
                    raise MeasurementError("SSE stream ended with an incomplete event")
                return None
            chunk = await self._read_chunk()
            if not chunk:
                if self.finished:
                    continue
                continue
            self.buffer.extend(chunk)
            self.peak_buffer_bytes = max(self.peak_buffer_bytes, len(self.buffer))
            if len(self.buffer) > MAX_SSE_EVENT_BYTES:
                raise MeasurementError("SSE event exceeds the bounded reader buffer")


@dataclasses.dataclass
class BurstState:
    """Bounded request accounting and barriers for one burst."""

    burst_id: str
    expected: set[str]
    slow_reader: bool = False
    started: set[str] = dataclasses.field(default_factory=set)
    active: set[str] = dataclasses.field(default_factory=set)
    peak_active: int = 0
    peak_client_active: int = 0
    finished: set[str] = dataclasses.field(default_factory=set)
    cancelled: set[str] = dataclasses.field(default_factory=set)
    invalid: list[str] = dataclasses.field(default_factory=list)
    changed: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    release: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    cancel_clients: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    slow_start: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)

    def note_change(self) -> None:
        self.changed.set()

    async def wait_for(self, predicate: Any, timeout: float, message: str) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while not predicate():
            self.changed.clear()
            if predicate():
                break
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise MeasurementError(message)
            try:
                await asyncio.wait_for(self.changed.wait(), remaining)
            except TimeoutError as error:
                raise MeasurementError(message) from error


class Backend:
    """Test-only asynchronous OpenAI SSE backend with bounded barriers."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.server: asyncio.AbstractServer | None = None
        self.port = 0
        self.host = "127.0.0.1"
        self.states: dict[str, BurstState] = {}
        self.warm_started = 0
        self.warm_finished = 0
        self.errors: list[str] = []
        self.client_active: set[str] = set()
        self.handler_tasks: set[asyncio.Task[Any]] = set()
        self.writers: set[asyncio.StreamWriter] = set()

    async def start(self, host: str = "127.0.0.1") -> None:
        self.host = host
        self.server = await asyncio.start_server(self.handle, host, 0, backlog=2048, limit=MAX_HTTP_HEAD_BYTES)
        sockets = self.server.sockets or []
        if not sockets:
            raise MeasurementError("backend server did not bind a socket")
        self.port = int(sockets[0].getsockname()[1])

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        for state in self.states.values():
            state.release.set()
            state.cancel_clients.set()
            state.slow_start.set()
        for writer in tuple(self.writers):
            writer.close()
        if self.handler_tasks:
            _done, pending = await asyncio.wait(self.handler_tasks, timeout=5)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    def active_count(self) -> int:
        return sum(len(state.active) for state in self.states.values())

    async def read_request(self, reader: asyncio.StreamReader) -> tuple[dict[str, str], bytes]:
        raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=15)
        if len(raw) > MAX_HTTP_HEAD_BYTES:
            raise MeasurementError("backend received an oversized request head")
        lines = raw[:-4].split(b"\r\n")
        request_line = lines[0].split() if lines else []
        if len(request_line) < 2 or request_line[0] != b"POST" or request_line[1] != b"/v1/chat/completions":
            raise MeasurementError("backend received an unexpected inference request path")
        headers: dict[str, str] = {}
        for line in lines[1:]:
            name, separator, value = line.partition(b":")
            if not separator:
                raise MeasurementError("backend received a malformed request header")
            headers[name.decode("latin-1").strip().lower()] = value.decode("latin-1").strip()
        length = int(headers.get("content-length", "0"))
        if length < 0 or length > MAX_HTTP_HEAD_BYTES:
            raise MeasurementError("backend request body is outside the bounded test envelope")
        body = await asyncio.wait_for(reader.readexactly(length), timeout=15) if length else b""
        return headers, body

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        request_id = "unknown"
        state: BurstState | None = None
        current = asyncio.current_task()
        if current is not None:
            self.handler_tasks.add(current)
        self.writers.add(writer)
        try:
            headers, body = await self.read_request(reader)
            phase = headers.get("x-qualification-phase", "")
            request_id = headers.get("x-qualification-request", "unknown")
            if headers.get("x-qualification-run") != self.run_id:
                raise MeasurementError("backend request did not carry the current run identifier")
            if b'"model":"llama"' not in body:
                raise MeasurementError("backend request did not carry the expected OpenAI model")
            if phase == "warmup":
                self.warm_started += 1
            burst_id = headers.get("x-qualification-burst", "")
            state = self.states.get(burst_id)
            if state is None:
                raise MeasurementError("backend request named an unknown burst")
            if request_id not in state.expected or request_id in state.started:
                state.invalid.append(request_id)
                state.note_change()
                raise MeasurementError("backend request ID is unexpected or duplicated")
            state.started.add(request_id)
            state.active.add(request_id)
            state.peak_active = max(state.peak_active, len(state.active))
            state.note_change()
            await self.send_head(writer)
            content = "warm" if phase == "warmup" else "x"
            await self.send_chunk(writer, openai_sse_delta(content))
            if state.slow_reader:
                await state.slow_start.wait()
                event_size = SLOW_READER_EVENT_BYTES
                filler_size = event_size - len(openai_sse_delta(""))
                if filler_size <= 0:
                    raise MeasurementError("slow-reader event size is too small for the SSE envelope")
                payload = openai_sse_delta("x" * filler_size)
                if len(payload) != event_size:
                    raise MeasurementError("slow-reader SSE event does not match the configured byte size")
                for _ in range(SLOW_READER_EVENTS):
                    await self.send_chunk(writer, payload)
                    await asyncio.sleep(SLOW_READER_EVENT_INTERVAL_SECONDS)
                await self.send_chunk(writer, b"data: [DONE]\n\n")
                await self.send_chunk(writer, b"")
                state.finished.add(request_id)
                state.active.discard(request_id)
                state.note_change()
                return
            await self.wait_for_release_or_disconnect(reader, writer, state, request_id)
            if phase == "warmup" and request_id in state.finished:
                self.warm_finished += 1
        except (
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
            ConnectionError,
            OSError,
            TimeoutError,
            ValueError,
        ) as error:
            if state is not None and request_id in state.active:
                state.cancelled.add(request_id)
                state.active.discard(request_id)
                state.note_change()
            self.remember_error(f"{request_id}: {type(error).__name__}")
        except MeasurementError as error:
            self.remember_error(str(error))
        finally:
            if state is not None and request_id in state.active:
                state.cancelled.add(request_id)
                state.active.discard(request_id)
                state.note_change()
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            self.writers.discard(writer)
            if current is not None:
                self.handler_tasks.discard(current)

    async def wait_for_release_or_disconnect(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        state: BurstState,
        request_id: str,
    ) -> None:
        disconnect = asyncio.create_task(reader.read(1))
        release = asyncio.create_task(state.release.wait())
        try:
            while True:
                done, _pending = await asyncio.wait(
                    {disconnect, release}, timeout=1.0, return_when=asyncio.FIRST_COMPLETED
                )
                if disconnect in done:
                    data = disconnect.result()
                    if not data:
                        state.cancelled.add(request_id)
                        state.active.discard(request_id)
                        state.note_change()
                        return
                    disconnect = asyncio.create_task(reader.read(1))
                if release in done:
                    break
                if not done:
                    await self.send_chunk(writer, b": keepalive\n\n")
            await self.send_chunk(writer, b"data: [DONE]\n\n")
            await self.send_chunk(writer, b"")
            state.finished.add(request_id)
        finally:
            for task in (disconnect, release):
                if not task.done():
                    task.cancel()
            await asyncio.gather(disconnect, release, return_exceptions=True)
            state.active.discard(request_id)
            state.note_change()

    async def send_head(self, writer: asyncio.StreamWriter) -> None:
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream\r\n"
            b"Cache-Control: no-cache\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"Connection: close\r\n\r\n"
        )
        await writer.drain()

    async def send_chunk(self, writer: asyncio.StreamWriter, body: bytes) -> None:
        if not body:
            writer.write(b"0\r\n\r\n")
        else:
            writer.write(f"{len(body):x}\r\n".encode() + body + b"\r\n")
        await writer.drain()

    def remember_error(self, message: str) -> None:
        if len(self.errors) < 100:
            self.errors.append(message[:500])


class GatewayContainer:
    """Own one task-named Gateway container and its bounded logs."""

    def __init__(
        self,
        image: str,
        variant: str,
        run_id: str,
        evidence: Path,
        backend_host: str,
        backend_port: int,
        source: dict[str, Any],
    ):
        self.image = image
        self.variant = variant
        self.run_id = run_id
        self.evidence = evidence
        self.backend_host = backend_host
        self.backend_port = backend_port
        self.source = source
        self.measurement_uid = os.getuid()
        self.measurement_gid = os.getgid()
        self.name = f"grid-memory-{run_id[:12]}-{variant}"
        self.host_port = 0
        self.pid = 0
        self.start_ticks = 0
        self.allowed_cpus: list[int] = []
        self.image_data: dict[str, Any] = {}
        self.image_version = ""
        self.binary_sha256 = ""
        self.container_data: dict[str, Any] = {}
        self.container_created = False
        self.container_id: str | None = None
        self.cidfile = evidence / "gateway-container.cid"
        self.log_process: subprocess.Popen[bytes] | None = None
        self.log_thread: threading.Thread | None = None
        self.log_error: str | None = None
        self.startup_line = ""
        self.cpuset = self.choose_cpu_set()

    @staticmethod
    def choose_cpu_set() -> str:
        if not hasattr(os, "sched_getaffinity"):
            raise MeasurementError("CPU affinity is required to hold worker count constant")
        cpus = sorted(os.sched_getaffinity(0))
        if len(cpus) < 4:
            raise MeasurementError("the fixed stream-memory profile requires at least four available CPUs")
        chosen = cpus[:4]
        return ",".join(str(cpu) for cpu in chosen)

    def docker(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        try:
            result = subprocess.run(
                ["docker", *args],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=60,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise MeasurementError(f"docker command failed to run: {type(error).__name__}") from error
        if check and result.returncode != 0:
            raise MeasurementError(f"docker {' '.join(args[:3])} failed: {result.stderr.strip()[:1000]}")
        return result

    def inspect_image(self) -> None:
        result = self.docker("image", "inspect", self.image)
        try:
            image_data = json.loads(result.stdout)[0]
        except (json.JSONDecodeError, IndexError, TypeError) as error:
            raise MeasurementError("Docker returned invalid image metadata") from error
        labels = (image_data.get("Config") or {}).get("Labels") or {}
        validate_image_labels(labels, self.source, self.variant)
        self.image_data = {
            "reference": self.image,
            "id": image_data.get("Id"),
            "repo_digests": image_data.get("RepoDigests") or [],
            "repo_tags": image_data.get("RepoTags") or [],
            "architecture": image_data.get("Architecture"),
            "os": image_data.get("Os"),
            "labels": labels,
        }
        image_id = self.image_data.get("id")
        validate_immutable_image_reference(self.image, image_id, self.image_data["repo_digests"])
        if self.image_data.get("architecture") != "amd64" or self.image_data.get("os") != "linux":
            raise MeasurementError("the stream-memory qualification requires a linux/amd64 Gateway image")
        version = self.docker("run", "--rm", "--entrypoint", "/usr/local/bin/grid-gateway", self.image, "--version")
        binary = self.docker("run", "--rm", "--entrypoint", "/usr/bin/sha256sum", self.image, "/usr/local/bin/grid-gateway", check=False)
        if binary.returncode != 0:
            binary = self.docker("run", "--rm", "--entrypoint", "/bin/sh", self.image, "-c", "sha256sum /usr/local/bin/grid-gateway")
        libc = self.docker(
            "run", "--rm", "--entrypoint", "/usr/bin/ldd", self.image, *libc_identification_args(self.variant)
        )
        dependencies = self.docker(
            "run", "--rm", "--entrypoint", "/usr/bin/ldd", self.image, "/usr/local/bin/grid-gateway", check=False
        )
        os_release = self.docker("run", "--rm", "--entrypoint", "/bin/cat", self.image, "/etc/os-release")
        match = re.match(r"([0-9a-f]{64})\s", binary.stdout)
        if match is None:
            raise MeasurementError("could not calculate the Gateway binary SHA-256 in the image")
        self.image_version = version.stdout.strip()
        self.binary_sha256 = match.group(1)
        self.image_data["binary_sha256"] = self.binary_sha256
        self.image_data["version_output"] = self.image_version
        self.image_data["libc_version_output"] = libc.stdout.strip()
        self.image_data["binary_dynamic_dependencies"] = (dependencies.stdout + dependencies.stderr).strip()
        self.image_data["runtime_os_release"] = os_release.stdout.strip()
        libc_version = (libc.stdout + libc.stderr).lower()
        expected_libc = "glibc" if self.variant == "gnu-jemalloc" else "musl"
        if expected_libc not in libc_version:
            raise MeasurementError(f"image runtime does not identify expected {expected_libc} libc")
        dependency_output = (dependencies.stdout + dependencies.stderr).lower()
        if "not found" in dependency_output:
            raise MeasurementError("Gateway binary has an unresolved runtime library dependency")
        if dependencies.returncode != 0 and not (
            self.variant == "musl-malloc"
            and ("static" in dependency_output or "not a dynamic executable" in dependency_output)
        ):
            raise MeasurementError("could not resolve Gateway binary runtime dependencies")

    async def start(self, serving_path: Path, praxis_path: Path) -> None:
        self.inspect_image()
        args = [
            "create",
            "--cidfile",
            str(self.cidfile),
            "--name",
            self.name,
            "--restart=no",
            "--stop-signal",
            "SIGINT",
            "--cpuset-cpus",
            self.cpuset,
            "--memory",
            MEMORY_LIMIT_TEXT,
            "--memory-swap",
            MEMORY_LIMIT_TEXT,
            "--pids-limit",
            "4096",
            "--user",
            measurement_user(self.measurement_uid, self.measurement_gid),
            "--ulimit",
            "nofile=8192:8192",
            *docker_log_options(),
            "--publish",
            "127.0.0.1::8080/tcp",
            "--add-host",
            f"host.docker.internal:{self.backend_host}",
            "--mount",
            f"type=bind,src={serving_path},dst=/run/grid-memory/serving.json,readonly",
            "--mount",
            f"type=bind,src={praxis_path},dst=/run/grid-memory/praxis.yaml,readonly",
            "--env",
            "GRID_SERVING_CONFIG=/run/grid-memory/serving.json",
            "--env",
            "RUST_LOG=info",
        ]
        if self.variant == "gnu-jemalloc":
            args.extend(["--env", "_RJEM_MALLOC_CONF=stats_print:true"])
        args.extend([
            "--entrypoint",
            "/usr/local/bin/grid-gateway",
            self.image,
            "--config",
            "/run/grid-memory/praxis.yaml",
        ])
        try:
            self.docker(*args)
        finally:
            # Docker writes the owned ID before starting; even a failed create can leave an object.
            if self.cidfile.exists():
                container_id = self.cidfile.read_text().strip()
                if re.fullmatch(r"[0-9a-f]{64}", container_id) is None:
                    raise MeasurementError("Docker cidfile contains an invalid container ID")
                self.container_id = container_id
                self.container_created = True
        if self.container_id is None:
            raise MeasurementError("Docker did not record the created Gateway container ID")
        self.docker("start", self.container_id)
        self.begin_log_capture()
        port_output = self.docker("port", self.container_id, "8080/tcp").stdout.strip()
        match = re.search(r":([0-9]+)$", port_output)
        if match is None:
            raise MeasurementError("Docker did not publish the Gateway listener")
        self.host_port = int(match.group(1))
        await self.wait_ready()
        self.inspect_process()
        await self.wait_startup_identity()

    def begin_log_capture(self) -> None:
        self.log_process = subprocess.Popen(
            ["docker", "logs", "--follow", self.container_id], stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        if self.log_process.stdout is None:
            raise MeasurementError("could not capture Gateway logs")
        self.log_thread = threading.Thread(target=self.copy_logs, args=(self.log_process.stdout,), daemon=True)
        self.log_thread.start()

    def copy_logs(self, stream: Any) -> None:
        log_path = self.evidence / "gateway.log"
        tail: collections.deque[bytes] = collections.deque()
        tail_bytes = 0
        main_bytes = 0
        read_chunk = getattr(stream, "read1", stream.read)
        try:
            with log_path.open("wb") as output:
                while True:
                    chunk = read_chunk(64 * 1024)
                    if not chunk:
                        break
                    if main_bytes < MAX_GATEWAY_LOG_BYTES:
                        kept = chunk[: MAX_GATEWAY_LOG_BYTES - main_bytes]
                        output.write(kept)
                        output.flush()
                        main_bytes += len(kept)
                    tail.append(chunk)
                    tail_bytes += len(chunk)
                    while tail and tail_bytes > MAX_GATEWAY_LOG_TAIL_BYTES:
                        removed = tail.popleft()
                        excess = tail_bytes - MAX_GATEWAY_LOG_TAIL_BYTES
                        if len(removed) > excess:
                            tail.appendleft(removed[excess:])
                            tail_bytes -= excess
                        else:
                            tail_bytes -= len(removed)
                if tail_bytes and main_bytes >= MAX_GATEWAY_LOG_BYTES:
                    output.write(b"\n[log limit reached; last lines follow in gateway-tail.log]\n")
            if tail and main_bytes >= MAX_GATEWAY_LOG_BYTES:
                with (self.evidence / "gateway-tail.log").open("wb") as tail_file:
                    tail_file.writelines(tail)
        except OSError as error:
            self.log_error = f"Gateway log capture failed: {type(error).__name__}"

    async def wait_ready(self) -> None:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            state = self.docker("inspect", "--format", "{{.State.Running}}", self.container_id, check=False)
            if state.returncode != 0 or state.stdout.strip() != "true":
                raise MeasurementError("Gateway container exited before opening its listener")
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection("127.0.0.1", self.host_port), timeout=1
                )
                writer.close()
                await writer.wait_closed()
                return
            except (OSError, TimeoutError):
                await asyncio.sleep(0.25)
        raise MeasurementError("Gateway listener did not become ready within 60 seconds")

    def inspect_process(self) -> None:
        raw = self.docker("inspect", self.container_id).stdout
        try:
            self.container_data = json.loads(raw)[0]
        except (json.JSONDecodeError, IndexError, TypeError) as error:
            raise MeasurementError("Docker returned invalid container metadata") from error
        self.pid = int((self.container_data.get("State") or {}).get("Pid") or 0)
        if self.pid <= 0:
            raise MeasurementError("Gateway container has no host process ID")
        if self.container_data.get("Image") != self.image_data.get("id"):
            raise MeasurementError("running container image ID differs from the inspected immutable image")
        proc = Path(f"/proc/{self.pid}")
        try:
            executable = os.readlink(proc / "exe")
            command = (proc / "cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
            status = (proc / "status").read_text()
            maps = (proc / "maps").read_text(errors="replace")
            self.start_ticks = parse_proc_start_ticks((proc / "stat").read_text())
        except (OSError, ValueError) as error:
            raise MeasurementError(f"cannot identify the Gateway process from /proc: {type(error).__name__}") from error
        if not executable.endswith("/grid-gateway") or "grid-gateway" not in command:
            raise MeasurementError(f"container PID {self.pid} is not the Gateway executable")
        validate_gateway_user(status, self.measurement_uid, self.measurement_gid)
        expected_cpus = set(parse_cpu_set(self.cpuset))
        try:
            actual_cpus = os.sched_getaffinity(self.pid)
        except OSError as error:
            raise MeasurementError(f"cannot read the Gateway CPU affinity: {type(error).__name__}") from error
        if actual_cpus != expected_cpus:
            raise MeasurementError("Gateway CPU affinity differs from the fixed qualification cpuset")
        self.allowed_cpus = sorted(actual_cpus)
        if self.variant == "gnu-jemalloc" and "libc.so.6" not in maps:
            raise MeasurementError("GNU qualification image process does not map glibc")
        if self.variant == "musl-malloc" and "ld-musl" not in maps:
            if "musl" not in self.image_data.get("libc_version_output", "").lower():
                raise MeasurementError("musl qualification process has no mapped or identified musl runtime")

    async def wait_startup_identity(self) -> None:
        expected_target = "x86_64-unknown-linux-gnu" if self.variant == "gnu-jemalloc" else "x86_64-unknown-linux-musl"
        expected_commit = self.source["commit"]
        deadline = time.monotonic() + 30
        log_path = self.evidence / "gateway.log"
        while time.monotonic() < deadline:
            try:
                log = log_path.read_text(errors="replace")
            except OSError:
                log = ""
            line = next((item for item in log.splitlines() if "starting grid-gateway" in item), "")
            if line:
                self.startup_line = line
                if expected_commit not in line or expected_target not in line:
                    raise MeasurementError("Gateway startup identity does not match its source or libc variant")
                return
            await asyncio.sleep(0.1)
        raise MeasurementError("Gateway startup identity was not recorded in the bounded log")

    def proc_sample(self) -> dict[str, Any]:
        proc = Path(f"/proc/{self.pid}")
        try:
            start_ticks = parse_proc_start_ticks((proc / "stat").read_text())
            if start_ticks != self.start_ticks:
                raise MeasurementError("Gateway PID was reused during the measurement")
            smaps: str | None
            try:
                smaps = (proc / "smaps_rollup").read_text()
            except OSError:
                smaps = None
            status = (proc / "status").read_text()
            rss_bytes, rss_source = parse_process_rss(smaps, status)
        except (OSError, ValueError) as error:
            raise MeasurementError(f"Gateway process RSS sample failed: {type(error).__name__}") from error
        status_fields = {line.partition(":")[0]: line.partition(":")[2].strip() for line in status.splitlines()}
        try:
            threads = int(status_fields["Threads"])
        except (KeyError, ValueError) as error:
            raise MeasurementError("Gateway thread count is absent or invalid") from error
        if threads <= 0:
            raise MeasurementError("Gateway thread count must be positive")
        return {
            "gateway_pid": self.pid,
            "gateway_start_ticks": start_ticks,
            "rss_bytes": rss_bytes,
            "rss_source": rss_source,
            "gateway_threads": threads,
            "cpu_allowed_list": status_fields.get("Cpus_allowed_list", ""),
        }

    def stop(self) -> dict[str, Any]:
        cleanup_errors: list[str] = []
        stop_return_code: int | None = None
        stop_stderr = ""
        post_stop_running: bool | None = None
        remove_return_code: int | None = None
        remove_stderr = ""
        post_remove_absent: bool | None = None
        if self.container_created:
            try:
                stopped = self.docker("stop", "--time", "15", self.container_id, check=False)
                stop_return_code = stopped.returncode
                stop_stderr = stopped.stderr.strip()[:1000]
            except MeasurementError as error:
                cleanup_errors.append(f"container stop failed: {type(error).__name__}")
            try:
                inspected = self.docker("inspect", self.container_id, check=False)
                data = json.loads(inspected.stdout or "[]")
                if inspected.returncode != 0 or not data:
                    raise ValueError("inspect returned no container")
                self.container_data = data[0]
                state = self.container_data.get("State") or {}
                post_stop_running = state.get("Running")
            except (json.JSONDecodeError, IndexError, TypeError, MeasurementError):
                cleanup_errors.append("container state could not be inspected after stop")
            except ValueError:
                cleanup_errors.append("container state could not be inspected after stop")
            if self.log_process is not None:
                try:
                    self.log_process.terminate()
                    self.log_process.wait(timeout=5)
                except ProcessLookupError:
                    pass
                except subprocess.TimeoutExpired:
                    self.log_process.kill()
                    try:
                        self.log_process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        cleanup_errors.append("Gateway log follower did not exit after kill")
            if self.log_thread is not None:
                self.log_thread.join(timeout=5)
                if self.log_thread.is_alive():
                    cleanup_errors.append("Gateway log capture thread did not stop")
        result = {
            "container_name": self.name,
            "container_id": self.container_id,
            "container_created": self.container_created,
            "stop_return_code": stop_return_code,
            "stop_stderr": stop_stderr,
            "post_stop_running": post_stop_running,
            "container_image_id": (self.container_data.get("Image") if self.container_data else None),
            "oom_killed": ((self.container_data.get("State") or {}).get("OOMKilled") if self.container_data else None),
            "exit_code": ((self.container_data.get("State") or {}).get("ExitCode") if self.container_data else None),
            "measurement_uid": self.measurement_uid,
            "measurement_gid": self.measurement_gid,
            "cpuset": self.cpuset,
            "allowed_cpus": self.allowed_cpus,
            "memory_limit_bytes": MEMORY_LIMIT_BYTES,
            "gateway_pid": self.pid,
            "gateway_start_ticks": self.start_ticks,
            "startup_identity": self.startup_line,
            "log_capture_error": self.log_error,
            "cleanup_errors": cleanup_errors,
        }
        if result["exit_code"] == 137:
            cleanup_errors.append("Gateway required SIGKILL after the stop timeout")
        if self.variant == "gnu-jemalloc" and self.container_created:
            try:
                gateway_log = (self.evidence / "gateway.log").read_text(errors="replace")
            except OSError:
                gateway_log = ""
            result["jemalloc_background_thread_verified"] = "opt.background_thread: true" in gateway_log
            if not result["jemalloc_background_thread_verified"]:
                cleanup_errors.append("GNU image did not emit jemalloc background-thread statistics on exit")
        if self.container_created:
            try:
                removed = self.docker("rm", "--force", self.container_id, check=False)
                remove_return_code = removed.returncode
                remove_stderr = removed.stderr.strip()[:1000]
            except MeasurementError as error:
                cleanup_errors.append(f"container removal failed: {type(error).__name__}")
            try:
                inspected = self.docker("inspect", self.container_id, check=False)
                if inspected.returncode == 0:
                    post_remove_absent = False
                elif re.search(r"(?i)no such (?:object|container)", inspected.stderr):
                    post_remove_absent = True
                else:
                    post_remove_absent = None
            except MeasurementError:
                post_remove_absent = None
            self.container_created = False
        result["remove_return_code"] = remove_return_code
        result["remove_stderr"] = remove_stderr
        result["post_remove_absent"] = post_remove_absent
        cleanup_errors.extend(gateway_cleanup_errors(result))
        result["cleanup_errors"] = cleanup_errors
        return result


class Sampler:
    """Write one-second process and active-stream observations to JSONL."""

    def __init__(self, gateway: GatewayContainer, backend: Backend, evidence: Path) -> None:
        self.gateway = gateway
        self.backend = backend
        self.evidence = evidence
        self.phase = "startup"
        self.burst_id: str | None = None
        self.started_at = time.monotonic()
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.task: asyncio.Task[None] | None = None
        self.file = (evidence / "samples.jsonl").open("w", encoding="utf-8")

    async def start(self) -> None:
        self.record()
        self.task = asyncio.create_task(self.loop())

    def set_phase(self, phase: str, burst_id: str | None = None) -> None:
        self.phase = phase
        self.burst_id = burst_id
        self.record()

    def record(self) -> None:
        try:
            sample = {
                "timestamp_utc": utc_now(),
                "elapsed_seconds": round(time.monotonic() - self.started_at, 3),
                "phase": self.phase,
                "burst_id": self.burst_id,
                **self.gateway.proc_sample(),
                "client_active": len(self.backend.client_active),
                "backend_active": self.backend.active_count(),
            }
            self.samples.append(sample)
            self.file.write(json.dumps(sample, sort_keys=True) + "\n")
            self.file.flush()
        except (MeasurementError, OSError, ValueError) as error:
            message = f"sample failure: {type(error).__name__}: {error}"
            if len(self.errors) < 100:
                self.errors.append(message[:500])

    async def loop(self) -> None:
        while True:
            await asyncio.sleep(SAMPLE_PERIOD_SECONDS)
            self.record()

    async def close(self) -> None:
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        self.record()
        self.file.close()

    def phase_samples(self, phase: str, burst_id: str | None = None) -> list[dict[str, Any]]:
        return [
            sample
            for sample in self.samples
            if sample["phase"] == phase and (burst_id is None or sample["burst_id"] == burst_id)
        ]


class StreamMemoryRunner:
    """Own one source-identified run and write all raw/summary evidence."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.repo = Path(__file__).resolve().parents[1]
        self.source = git_identity(self.repo)
        self.run_id = args.run_id or uuid.uuid4().hex
        self.evidence = Path(args.evidence_dir).expanduser().resolve()
        if len(self.run_id) > 64 or not re.fullmatch(r"[A-Za-z0-9._-]+", self.run_id):
            raise MeasurementError("run ID must contain at most 64 safe filename characters")
        self.evidence.mkdir(parents=True, exist_ok=False)
        self.backend = Backend(self.run_id)
        self.gateway: GatewayContainer | None = None
        self.sampler: Sampler | None = None
        self.client_active = self.backend.client_active
        self.request_tasks: dict[str, list[asyncio.Task[dict[str, Any]]]] = {}
        self.bursts: list[dict[str, Any]] = []
        self.failures: list[str] = []
        self.container_cleanup: dict[str, Any] | None = None
        self._main_task: asyncio.Task[int] | None = None
        self._sigterm_received = False
        self._sigterm_cancel_requested = False
        self._cleanup_started = False
        self.baseline: int | None = None
        self.client_peak_sse_buffer_bytes = 0
        self.summary: dict[str, Any] = {}
        self.started_at = utc_now()
        self.host = self.host_identity()
        self.write_manifest()

    @staticmethod
    def host_identity() -> dict[str, Any]:
        cpus = sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else []
        memory_values: dict[str, int] = {}
        try:
            with Path("/proc/meminfo").open(encoding="utf-8") as memory:
                for line in memory:
                    key, separator, value = line.partition(":")
                    if separator and key in {"MemTotal", "MemAvailable"}:
                        tokens = value.split()
                        if len(tokens) >= 2 and tokens[1] == "kB":
                            memory_values[key] = int(tokens[0]) * 1024
        except (OSError, ValueError):
            memory_values = {}
        try:
            disk_free = shutil.disk_usage(Path(__file__).resolve().parents[1]).free
        except OSError:
            disk_free = None
        return {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "kernel": platform.release(),
            "python": platform.python_version(),
            "cpu_affinity_before_container": cpus,
            "memory_total_bytes": memory_values.get("MemTotal"),
            "memory_available_bytes": memory_values.get("MemAvailable"),
            "disk_free_bytes": disk_free,
            "measurement_uid": os.getuid(),
            "measurement_gid": os.getgid(),
        }

    def write_json(self, name: str, value: Any) -> None:
        path = self.evidence / name
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, path)

    def write_manifest(self) -> None:
        self.write_json(
            "manifest.json",
            {
                "schema_version": 1,
                "run_id": self.run_id,
                "created_at_utc": self.started_at,
                "mode": self.args.mode,
                "variant": self.args.variant,
                "image_reference": self.args.image,
                "source": self.source,
                "host": self.host,
                "command": ["python3", *sys.argv],
                "sensitive_data": False,
            },
        )

    async def run(self) -> int:
        if self.source["commit"] != self.args.source_revision:
            self.failures.append("--source-revision does not match the current checkout HEAD")
        if self.host["machine"] != "x86_64":
            self.failures.append("the fixed stream-memory profile currently supports x86_64 only")
        if self.failures:
            await self.write_final_summary()
            return 2
        loop = asyncio.get_running_loop()
        current_task = asyncio.current_task()
        if current_task is None:
            raise MeasurementError("stream-memory runner is not executing in an asyncio task")
        self._main_task = current_task
        loop.add_signal_handler(signal.SIGTERM, self.handle_sigterm)
        try:
            self.validate_host()
            await self.backend.start(self.host["docker_host_gateway"])
            serving, praxis = self.prepare_gateway_config()
            self.gateway = GatewayContainer(
                self.args.image,
                self.args.variant,
                self.run_id,
                self.evidence,
                self.backend.host,
                self.backend.port,
                self.source,
            )
            await self.gateway.start(serving, praxis)
            self.write_json("image-lock.json", self.gateway.image_data)
            self.sampler = Sampler(self.gateway, self.backend, self.evidence)
            await self.sampler.start()
            if self.args.mode in ("full", "observe", "quick"):
                await self.run_memory_profile()
            elif self.args.mode == "cancellation":
                await self.run_cancellation_profile()
            elif self.args.mode == "slow-reader":
                await self.run_slow_reader_profile()
        except (MeasurementError, OSError, ValueError, asyncio.TimeoutError, subprocess.SubprocessError) as error:
            self.failures.append(f"{type(error).__name__}: {str(error)[:1000]}")
        except KeyboardInterrupt:
            self.failures.append("interrupted by operator")
        except asyncio.CancelledError:
            if not self._sigterm_received:
                raise
            self.failures.append("interrupted by SIGTERM")
        finally:
            self._cleanup_started = True
            await self.release_requests()
            await self.backend.close()
            if self.sampler is not None:
                await self.sampler.close()
            if self.gateway is not None:
                self.container_cleanup = self.gateway.stop()
                if self.container_cleanup.get("oom_killed"):
                    self.failures.append("Gateway container was OOM-killed")
                if self.container_cleanup.get("log_capture_error"):
                    self.failures.append(self.container_cleanup["log_capture_error"])
                if self.container_cleanup.get("cleanup_errors"):
                    self.failures.extend(self.container_cleanup["cleanup_errors"])
            if self._sigterm_received and "interrupted by SIGTERM" not in self.failures:
                self.failures.append("interrupted by SIGTERM")
            await self.write_final_summary()
            loop.remove_signal_handler(signal.SIGTERM)
            self._main_task = None
        return (
            0
            if self.summary.get("qualification_pass")
            or self.summary.get("harness_pass")
            or self.summary.get("observation_complete")
            else 2
        )

    def handle_sigterm(self) -> None:
        """Cancel the run once on SIGTERM so its normal cleanup path can finish."""
        if self._sigterm_received:
            return
        self._sigterm_received = True
        if not self._cleanup_started and not self._sigterm_cancel_requested and self._main_task is not None:
            self._sigterm_cancel_requested = True
            self._main_task.cancel()

    async def release_requests(self) -> None:
        """Release barriers and reap every client task on both success and failure paths."""
        for state in self.backend.states.values():
            state.release.set()
            state.cancel_clients.set()
            state.slow_start.set()
        tasks = [task for group in self.request_tasks.values() for task in group]
        if not tasks:
            return
        _done, pending = await asyncio.wait(tasks, timeout=10)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    def validate_host(self) -> None:
        if not sys.platform.startswith("linux"):
            raise MeasurementError("the process RSS qualification requires Linux /proc")
        measurement_user(os.getuid(), os.getgid())
        soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        validate_file_descriptor_limit(soft)
        result = subprocess.run(["docker", "info", "--format", "{{.ServerVersion}}"], capture_output=True, text=True)
        if result.returncode != 0:
            raise MeasurementError(f"Docker daemon is unavailable: {result.stderr.strip()[:500]}")
        docker_root = subprocess.run(["docker", "info", "--format", "{{.DockerRootDir}}"], capture_output=True, text=True)
        if docker_root.returncode != 0 or not docker_root.stdout.strip():
            raise MeasurementError("could not determine Docker storage location")
        self.host["docker_server_version"] = result.stdout.strip()
        self.host["docker_root_dir"] = docker_root.stdout.strip()
        bridge = subprocess.run(
            ["docker", "network", "inspect", "bridge", "--format", "{{(index .IPAM.Config 0).Gateway}}"],
            capture_output=True,
            text=True,
        )
        if bridge.returncode != 0:
            raise MeasurementError(f"could not determine the Docker bridge address: {bridge.stderr.strip()[:500]}")
        try:
            bridge_address = ipaddress.ip_address(bridge.stdout.strip())
        except ValueError as error:
            raise MeasurementError("Docker bridge gateway is not a valid IP address") from error
        if bridge_address.version != 4:
            raise MeasurementError("the local stream-memory backend requires an IPv4 Docker bridge")
        self.host["docker_host_gateway"] = str(bridge_address)
        try:
            self.host["docker_disk_free_bytes"] = shutil.disk_usage(docker_root.stdout.strip()).free
        except OSError as error:
            raise MeasurementError(f"could not inspect Docker storage capacity: {type(error).__name__}") from error
        if self.args.mode in ("full", "observe"):
            if (self.host.get("memory_available_bytes") or 0) < FULL_HOST_MEMORY_AVAILABLE_BYTES:
                raise MeasurementError("full mode requires at least 8 GiB host memory available before startup")
            if (self.host.get("docker_disk_free_bytes") or 0) < FULL_DISK_FREE_BYTES:
                raise MeasurementError("full mode requires at least 10 GiB free on Docker's storage filesystem")
        self.write_manifest()

    def prepare_gateway_config(self) -> tuple[Path, Path]:
        temp = self.evidence / "runtime-config"
        temp.mkdir(mode=0o755)
        serving = {
            "local_site": "local",
            "window_secs": 60,
            "load_window_ms": 30_000,
            "candidates": [
                {
                    "kind": "inference_model",
                    "name": "llama",
                    "site": "local",
                    "cluster": "pool-local",
                    "stable_id": "stream-memory-candidate",
                }
            ],
            "provider_hop_clusters": [],
            "provider_hop_sni": {},
            "peers": [],
        }
        serving_path = temp / "serving.json"
        serving_path.write_text(json.dumps(serving, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        endpoint = f"host.docker.internal:{self.backend.port}"
        praxis = (
            "insecure_options:\n"
            "  allow_private_endpoints: true\n"
            "  allow_private_upstreams: true\n"
            "listeners:\n"
            "  - name: default\n"
            "    address: 0.0.0.0:8080\n"
            "    filter_chains: [main]\n"
            "filter_chains:\n"
            "  - name: main\n"
            "    filters:\n"
            "      - filter: grid_site_route\n"
            "        model_header: X-Model\n"
            "      - filter: load_balancer\n"
            "        clusters:\n"
            "          - name: pool-local\n"
            f"            endpoints: [\"{endpoint}\"]\n"
        )
        praxis_path = temp / "praxis.yaml"
        praxis_path.write_text(praxis, encoding="utf-8")
        os.chmod(serving_path, 0o644)
        os.chmod(praxis_path, 0o644)
        return serving_path, praxis_path

    async def warm_gateway(self) -> None:
        warm_count = FULL_WARMUP_CONCURRENCY if self.args.mode in ("full", "observe") else 4
        if self.sampler is not None:
            self.sampler.set_phase("warmup_live", "warmup")
        state = await self.start_burst("warmup", warm_count, slow_reader=False, phase="warmup")
        state.release.set()
        results = await asyncio.wait_for(
            asyncio.gather(*self.request_tasks["warmup"], return_exceptions=True), timeout=30
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise MeasurementError(f"warm-up failed: {errors[0]}")
        await state.wait_for(lambda: not state.active, 30, "warm-up backend did not drain")
        if self.backend.warm_started != warm_count or self.backend.warm_finished != warm_count:
            raise MeasurementError(
                f"warm-up backend receipts {self.backend.warm_started}/{self.backend.warm_finished} did not equal {warm_count}"
            )
        self.summary["warmup"] = {
            "requested_streams": warm_count,
            "backend_started": len(state.started),
            "backend_peak_live": state.peak_active,
            "client_peak_live": state.peak_client_active,
            "backend_finished": len(state.finished),
            "backend_cancelled": len(state.cancelled),
            "backend_active_after_drain": len(state.active),
            "client_active_after_drain": len(self.client_active),
        }

    async def run_memory_profile(self) -> None:
        if self.sampler is None:
            raise MeasurementError("sampler is not ready")
        full = self.args.mode in ("full", "observe")
        concurrency = FULL_CONCURRENCY if full else 8
        burst_count = FULL_BURSTS if full else 2
        plateau_seconds = FULL_PLATEAU_SECONDS if full else 2
        settle_seconds = FULL_BURST_SETTLE_SECONDS if full else 1
        final_idle_seconds = FULL_FINAL_IDLE_SECONDS if full else 5
        self.sampler.set_phase("cold_ready")
        await asyncio.sleep(FULL_BASELINE_SECONDS if full else 2)
        cold = self.window("cold_ready", RSS_SAMPLES if full else 1)
        self.summary["cold_ready_rss_median_bytes"] = median_bytes([item["rss_bytes"] for item in cold])
        await self.warm_gateway()
        self.sampler.set_phase("warm_settle")
        await asyncio.sleep(FULL_WARM_SETTLE_SECONDS if full else 2)
        warm = self.window("warm_settle", RSS_SAMPLES if full else 1)
        self.baseline = median_bytes([item["rss_bytes"] for item in warm])
        self.summary["warm_idle_baseline_rss_median_bytes"] = self.baseline
        self.summary["warm_idle_baseline_sample_count"] = len(warm)
        for index in range(burst_count):
            burst_id = f"burst-{index + 1:02d}"
            state = await self.start_burst(burst_id, concurrency, slow_reader=False)
            self.sampler.set_phase("burst_live", burst_id)
            await asyncio.sleep(plateau_seconds)
            live = self.sampler.phase_samples("burst_live", burst_id)
            if full and len(live) < plateau_seconds:
                raise MeasurementError(f"{burst_id} has too few RSS plateau samples: {len(live)}")
            if any(
                item["backend_active"] != concurrency or item["client_active"] != concurrency
                for item in live
            ):
                raise MeasurementError(f"{burst_id} RSS samples do not all show the requested live concurrency")
            live_rss = median_bytes([item["rss_bytes"] for item in live])
            self.sampler.set_phase("burst_release", burst_id)
            state.release.set()
            result = await asyncio.gather(*self.request_tasks[burst_id], return_exceptions=True)
            errors = [item for item in result if isinstance(item, BaseException)]
            if errors:
                raise MeasurementError(f"{burst_id} client failed: {errors[0]}")
            await state.wait_for(lambda: not state.active, 30, f"{burst_id} backend did not drain")
            if self.client_active:
                raise MeasurementError(f"{burst_id} clients did not return to zero")
            self.sampler.set_phase("burst_post_close", burst_id)
            post_close = self.gateway_sample()["rss_bytes"] if self.gateway is not None else 0
            self.sampler.set_phase("burst_settle", burst_id)
            await asyncio.sleep(settle_seconds)
            settled = self.sampler.phase_samples("burst_settle", burst_id)
            settle_rss = median_bytes([item["rss_bytes"] for item in settled[-RSS_SAMPLES:]])
            self.bursts.append(
                {
                    "burst_id": burst_id,
                    "requested_streams": concurrency,
                    "backend_started": len(state.started),
                    "backend_peak_live": state.peak_active,
                    "client_peak_live": state.peak_client_active,
                    "backend_finished": len(state.finished),
                    "backend_cancelled": len(state.cancelled),
                    "invalid_backend_receipts": state.invalid,
                    "backend_active_after_drain": len(state.active),
                    "client_active_after_drain": len(self.client_active),
                    "live_plateau_sample_count": len(live),
                    "live_plateau_rss_median_bytes": live_rss,
                    "immediate_post_close_rss_bytes": post_close,
                    "settle_rss_median_bytes": settle_rss,
                    "settle_sample_count": len(settled[-RSS_SAMPLES:]),
                    "memory_increment_bytes": live_rss - (self.baseline or 0),
                    "memory_per_confirmed_stream_bytes": (live_rss - (self.baseline or 0)) / concurrency,
                }
            )
            if len(state.started) != concurrency or len(state.finished) != concurrency or state.cancelled:
                raise MeasurementError(f"{burst_id} backend counts do not match the requested successful profile")
            self.sampler.set_phase("interburst_idle", burst_id)
            if index + 1 < burst_count:
                await asyncio.sleep(settle_seconds)
        self.sampler.set_phase("final_idle")
        await asyncio.sleep(final_idle_seconds)
        idle = self.sampler.phase_samples("final_idle")
        final_values = [item["rss_bytes"] for item in idle[-RSS_SAMPLES:]]
        if full and len(final_values) < RSS_SAMPLES:
            raise MeasurementError(f"final idle window has {len(final_values)} RSS samples")
        final_rss = median_bytes(final_values)
        if self.baseline is None:
            raise MeasurementError("warm idle baseline was not captured")
        ratio = ratio_value(self.baseline, final_rss)
        self.summary.update(
            {
                "warm_idle_baseline_rss_bytes": self.baseline,
                "final_idle_rss_median_bytes": final_rss,
                "final_to_warm_baseline_ratio": str(ratio),
                "max_settle_ratio": self.args.max_settle_ratio,
                "final_idle_samples": len(final_values),
                "per_burst": self.bursts,
                "stream_profile": {
                    "concurrency": concurrency,
                    "bursts": burst_count,
                    "plateau_seconds": plateau_seconds,
                    "interburst_settle_seconds": settle_seconds,
                    "final_idle_seconds": final_idle_seconds,
                },
            }
        )
        if self.args.mode == "full" and ratio_exceeded(self.baseline, final_rss, self.args.max_settle_ratio):
            raise MeasurementError(f"final settle ratio {ratio} exceeds {self.args.max_settle_ratio}")

    async def run_cancellation_profile(self) -> None:
        concurrency = 256
        cancel_count = concurrency // 4
        state = await self.start_burst("cancel-01", concurrency, slow_reader=False)
        self.sampler.set_phase("cancellation_live", "cancel-01")
        await asyncio.sleep(5)
        state.cancel_clients.set()
        await state.wait_for(lambda: len(state.cancelled) == cancel_count, 15, "backend did not observe client cancellation")
        state.release.set()
        results = await asyncio.gather(*self.request_tasks["cancel-01"], return_exceptions=True)
        errors = [item for item in results if isinstance(item, BaseException)]
        if errors:
            raise MeasurementError(f"cancellation profile client failed: {errors[0]}")
        await state.wait_for(lambda: not state.active, 30, "cancellation profile did not drain")
        if len(state.finished) != concurrency - cancel_count:
            raise MeasurementError("cancellation profile did not complete every surviving stream")
        self.summary["cancellation"] = {
            "requested_streams": concurrency,
            "cancel_requested": cancel_count,
            "backend_cancelled": len(state.cancelled),
            "backend_finished": len(state.finished),
            "backend_started": len(state.started),
            "remaining_client_active": len(self.client_active),
        }

    async def run_slow_reader_profile(self) -> None:
        concurrency = SLOW_READER_CONCURRENCY
        state = await self.start_burst("slow-reader-01", concurrency, slow_reader=True)
        self.sampler.set_phase("slow_reader_first_event", "slow-reader-01")
        await state.wait_for(lambda: len(self.client_active) == concurrency, 15, "slow-reader clients did not see the first SSE event")
        if len(state.finished):
            raise MeasurementError("backend completed before the slow-reader barrier released")
        active_before_continuation = len(state.active)
        if active_before_continuation != concurrency:
            raise MeasurementError(
                f"slow-reader backend had {active_before_continuation} active streams before continuation; expected {concurrency}"
            )
        state.slow_start.set()
        await asyncio.sleep(SLOW_READER_PAUSE_SECONDS / 2)
        if len(state.finished):
            raise MeasurementError("backend finished its stream burst while clients were paused")
        results = await asyncio.gather(*self.request_tasks["slow-reader-01"], return_exceptions=True)
        errors = [item for item in results if isinstance(item, BaseException)]
        if errors:
            raise MeasurementError(f"slow-reader client failed: {errors[0]}")
        await state.wait_for(lambda: not state.active, 30, "slow-reader backend did not drain")
        if len(state.finished) != concurrency:
            raise MeasurementError("slow-reader backend receipt count did not reconcile")
        self.summary["slow_reader"] = {
            "streams": concurrency,
            "clients_with_first_event_before_backend_continuation": concurrency,
            "backend_active_before_continuation": active_before_continuation,
            "backend_finished_before_continuation": 0,
            "events_per_stream": SLOW_READER_EVENTS,
            "event_bytes": SLOW_READER_EVENT_BYTES,
            "event_interval_seconds": SLOW_READER_EVENT_INTERVAL_SECONDS,
            "client_pause_seconds": SLOW_READER_PAUSE_SECONDS,
            "client_peak_sse_buffer_bytes": self.client_peak_sse_buffer_bytes,
            "backend_finished": len(state.finished),
            "backend_cancelled": len(state.cancelled),
            "backend_active_after_drain": len(state.active),
            "client_active_after_drain": len(self.client_active),
        }

    async def start_burst(
        self,
        burst_id: str,
        count: int,
        slow_reader: bool,
        phase: str = "stream",
    ) -> BurstState:
        if burst_id in self.backend.states:
            raise MeasurementError(f"duplicate burst ID {burst_id}")
        request_ids = {f"{burst_id}-request-{index:04d}" for index in range(count)}
        state = BurstState(burst_id=burst_id, expected=request_ids, slow_reader=slow_reader)
        self.backend.states[burst_id] = state
        self.request_tasks[burst_id] = [
            asyncio.create_task(
                self.client_request(request_id, phase, state, slow_reader=slow_reader, index=index)
            )
            for index, request_id in enumerate(sorted(request_ids))
        ]
        await state.wait_for(lambda: len(state.active) == count, 90, f"{burst_id} backend did not reach {count} live streams")
        await state.wait_for(lambda: len(self.client_active) == count, 30, f"{burst_id} clients did not confirm {count} live streams")
        if len(state.started) != count:
            raise MeasurementError(f"{burst_id} backend started {len(state.started)} of {count} requested streams")
        return state

    async def client_request(
        self,
        request_id: str,
        phase: str,
        state: BurstState | None,
        *,
        slow_reader: bool = False,
        index: int = 0,
    ) -> dict[str, Any]:
        if self.gateway is None:
            raise MeasurementError("Gateway container is not running")
        reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", self.gateway.host_port), timeout=20)
        body = json.dumps({"model": "llama", "stream": True, "messages": [{"role": "user", "content": request_id}]}, separators=(",", ":")).encode()
        burst_id = state.burst_id if state is not None else "warmup"
        request = (
            f"POST /v1/chat/completions HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{self.gateway.host_port}\r\n"
            "Content-Type: application/json\r\n"
            "Accept: text/event-stream\r\n"
            "X-Model: llama\r\n"
            f"X-Qualification-Run: {self.run_id}\r\n"
            f"X-Qualification-Burst: {burst_id}\r\n"
            f"X-Qualification-Request: {request_id}\r\n"
            f"X-Qualification-Phase: {phase}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n"
        ).encode() + body
        client_active = False
        try:
            writer.write(request)
            await asyncio.wait_for(writer.drain(), timeout=20)
            response_head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=30)
            status, headers = parse_http_head(response_head)
            if status != 200 or "text/event-stream" not in headers.get("content-type", "").lower():
                raise MeasurementError(f"{request_id} got HTTP {status} with content-type {headers.get('content-type')}")
            stream = HttpBody(reader, headers)
            first = await asyncio.wait_for(stream.next_event(), timeout=30)
            self.validate_sse_payload(first, request_id)
            self.client_active.add(request_id)
            client_active = True
            if state is not None:
                state.peak_client_active = max(state.peak_client_active, len(self.client_active))
                state.note_change()
            if phase == "warmup":
                if state is None:
                    raise MeasurementError("warm-up request has no stream barrier")
                await state.release.wait()
                await self._read_to_done(stream, request_id)
                self.record_client_buffer_peak(stream)
                return {"request_id": request_id, "status": status, "result": "finished"}
            if slow_reader:
                await asyncio.sleep(SLOW_READER_PAUSE_SECONDS)
            if state is not None and not slow_reader and state.burst_id.startswith("cancel-"):
                if index < len(state.expected) // 4:
                    await state.cancel_clients.wait()
                    writer.close()
                    await writer.wait_closed()
                    self.client_active.discard(request_id)
                    client_active = False
                    return {"request_id": request_id, "status": status, "result": "cancelled"}
            if state is not None and slow_reader:
                await self._read_to_done(stream, request_id)
            elif state is not None:
                await state.release.wait()
                await self._read_to_done(stream, request_id)
            self.record_client_buffer_peak(stream)
            return {"request_id": request_id, "status": status, "result": "finished"}
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            if client_active:
                self.client_active.discard(request_id)
                if state is not None:
                    state.note_change()

    async def _read_to_done(self, stream: HttpBody, request_id: str) -> None:
        event_count = 0
        while True:
            event = await asyncio.wait_for(stream.next_event(), timeout=30)
            if event is None:
                raise MeasurementError(f"{request_id} ended before the SSE terminal event")
            event_count += 1
            if event_count > SLOW_READER_EVENTS + 16:
                raise MeasurementError(f"{request_id} exceeded the bounded SSE event count")
            if event == "[DONE]":
                return
            self.validate_sse_payload(event, request_id)

    @staticmethod
    def validate_sse_payload(event: str | None, request_id: str) -> None:
        """Reject malformed JSON or non-chat-completion events from the backend."""
        try:
            payload = json.loads(event or "")
        except json.JSONDecodeError as error:
            raise MeasurementError(f"{request_id} received malformed JSON in its SSE stream") from error
        choices = payload.get("choices") if isinstance(payload, dict) else None
        if not isinstance(choices, list) or not choices:
            raise MeasurementError(f"{request_id} received an invalid OpenAI chat-completion SSE event")

    def record_client_buffer_peak(self, stream: HttpBody) -> None:
        self.client_peak_sse_buffer_bytes = max(
            self.client_peak_sse_buffer_bytes, stream.peak_buffer_bytes
        )

    def gateway_sample(self) -> dict[str, Any]:
        if self.gateway is None:
            raise MeasurementError("Gateway container is not running")
        return self.gateway.proc_sample()

    def window(self, phase: str, minimum: int) -> list[dict[str, Any]]:
        if self.sampler is None:
            raise MeasurementError("sampler is not initialized")
        values = self.sampler.phase_samples(phase)
        if len(values) < minimum:
            raise MeasurementError(f"{phase} has {len(values)} samples; requires at least {minimum}")
        return values[-minimum:]

    async def write_final_summary(self) -> None:
        sample_errors = self.sampler.errors if self.sampler is not None else []
        if sample_errors:
            self.failures.extend(sample_errors)
        if self.backend.errors:
            self.failures.extend(self.backend.errors)
        if self.client_active:
            self.failures.append(f"{len(self.client_active)} client stream(s) remain active at cleanup")
        full_profile = self.args.mode in ("full", "observe")
        qualification_mode = self.args.mode == "full"
        observation_mode = self.args.mode == "observe"
        sample_count = len(self.sampler.samples) if self.sampler is not None else 0
        final_rss = self.summary.get("final_idle_rss_median_bytes")
        evidence_errors = (
            validate_full_evidence(
                baseline=self.baseline,
                baseline_sample_count=self.summary.get("warm_idle_baseline_sample_count", 0),
                final_rss=final_rss if isinstance(final_rss, int) else None,
                final_idle_sample_count=self.summary.get("final_idle_samples", 0),
                bursts=self.bursts,
                sample_count=sample_count,
                ratio_limit=self.args.max_settle_ratio,
                require_ratio=qualification_mode,
                expected_image_id=(self.gateway.image_data.get("id") if self.gateway is not None else None),
                cleanup=self.container_cleanup,
                rss_sources=(
                    [sample.get("rss_source") for sample in self.sampler.samples]
                    if self.sampler is not None
                    else []
                ),
            )
            if full_profile
            else []
        )
        self.failures.extend(error for error in evidence_errors if error not in self.failures)
        if self.failures:
            outcome = "TRIAL_FAIL"
        elif qualification_mode:
            outcome = "TRIAL_PASS"
        elif observation_mode:
            outcome = "OBSERVATION_COMPLETE_NOT_QUALIFICATION"
        else:
            outcome = "HARNESS_PASS_NOT_QUALIFICATION"
        self.summary.update(
            {
                "schema_version": 1,
                "run_id": self.run_id,
                "started_at_utc": self.started_at,
                "finished_at_utc": utc_now(),
                "mode": self.args.mode,
                "variant": self.args.variant,
                "source": self.source,
                "image": (self.gateway.image_data if self.gateway is not None else None),
                "container_cleanup": self.container_cleanup,
                "sample_count": sample_count,
                "sampling_errors": sample_errors,
                "backend_errors": self.backend.errors,
                "failures": self.failures,
                "outcome": outcome,
                "qualification_pass": bool(qualification_mode and not self.failures),
                "harness_pass": bool(not full_profile and not self.failures),
                "observation_complete": bool(observation_mode and not self.failures),
                "qualification_claim": bool(qualification_mode and not self.failures),
                "sigterm_received": self._sigterm_received,
            }
        )
        self.write_json("bursts.json", self.bursts)
        self.write_json("summary.json", self.summary)
        if self.gateway is not None:
            self.write_json("container-cleanup.json", self.container_cleanup)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        required=True,
        choices=("full", "observe", "quick", "cancellation", "slow-reader"),
        help="observe runs the full workload and evidence checks without a ratio gate or qualification claim",
    )
    parser.add_argument("--variant", required=True, choices=("gnu-jemalloc", "musl-malloc"))
    parser.add_argument("--image", required=True, help="locally built immutable Gateway image reference")
    parser.add_argument("--source-revision", required=True, help="expected checkout HEAD baked into the image label")
    parser.add_argument("--max-settle-ratio", help="required for full mode; compared against warm idle baseline")
    parser.add_argument("--run-id", help="safe unique ID; generated when omitted")
    parser.add_argument("--evidence-dir", required=True, help="new, empty destination directory for this run")
    args = parser.parse_args(argv)
    if not is_safe_immutable_image_reference(args.image):
        parser.error("--image must be a Docker image ID or repository@sha256 digest")
    if not re.fullmatch(r"[0-9a-f]{40}", args.source_revision):
        parser.error("--source-revision must be a 40-character lowercase Git SHA")
    if args.mode == "full":
        if args.max_settle_ratio is None:
            parser.error("--max-settle-ratio is required for full mode")
        try:
            limit = decimal.Decimal(args.max_settle_ratio)
        except decimal.InvalidOperation:
            parser.error("--max-settle-ratio must be numeric")
        if not limit.is_finite() or limit <= 0:
            parser.error("--max-settle-ratio must be finite and positive")
    elif args.max_settle_ratio is not None:
        parser.error("--max-settle-ratio is only accepted with --mode full")
    return args


def main(argv: list[str] | None = None) -> int:
    actual_argv = sys.argv[1:] if argv is None else argv
    if actual_argv == ["--print-source-identity"]:
        repo = Path(__file__).resolve().parents[1]
        print(json.dumps(git_identity(repo), indent=2, sort_keys=True))
        return 0
    args = parse_args(actual_argv)
    try:
        runner = StreamMemoryRunner(args)
    except (MeasurementError, OSError, subprocess.SubprocessError) as error:
        print(f"stream-memory preflight failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 2
    code = asyncio.run(runner.run())
    print(f"{runner.summary.get('outcome')} evidence: {runner.evidence}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
