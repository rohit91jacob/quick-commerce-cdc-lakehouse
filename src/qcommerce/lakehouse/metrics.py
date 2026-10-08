"""In-process writer metrics, exposed over HTTP in Prometheus text format (plus JSON progress)."""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class WriterMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.started_at = time.time()
        self.batches = 0
        self.last_batch_id = -1
        self.last_batch_seconds = 0.0
        self.last_batch_finished_at = 0.0
        self.events: Counter[tuple[str, str]] = Counter()
        self.dead_letters = 0
        self.merge_retries = 0
        self.schema_changes = 0
        self.progress: dict[str, Any] = {}

    def record_batch(
        self,
        batch_id: int,
        seconds: float,
        events: Counter[tuple[str, str]],
        dead_letters: int,
        schema_changes: int,
    ) -> None:
        with self._lock:
            self.batches += 1
            self.last_batch_id = batch_id
            self.last_batch_seconds = seconds
            self.last_batch_finished_at = time.time()
            self.events.update(events)
            self.dead_letters += dead_letters
            self.schema_changes += schema_changes

    def record_retry(self) -> None:
        with self._lock:
            self.merge_retries += 1

    def record_progress(self, progress: dict[str, Any]) -> None:
        with self._lock:
            self.progress = progress

    def offsets_behind(self) -> tuple[float, float]:
        """(max, avg) Kafka offsets behind latest, from the last streaming progress report."""
        maxima, averages = [], []
        for source in self.progress.get("sources", []):
            m = source.get("metrics") or {}
            if "maxOffsetsBehindLatest" in m:
                maxima.append(float(m["maxOffsetsBehindLatest"]))
                averages.append(float(m.get("avgOffsetsBehindLatest", 0)))
        return (max(maxima) if maxima else 0.0, max(averages) if averages else 0.0)

    def prometheus(self) -> str:
        with self._lock:
            behind_max, behind_avg = self.offsets_behind()
            lines = [
                "# TYPE qc_writer_batches_total counter",
                f"qc_writer_batches_total {self.batches}",
                "# TYPE qc_writer_last_batch_id gauge",
                f"qc_writer_last_batch_id {self.last_batch_id}",
                "# TYPE qc_writer_last_batch_duration_seconds gauge",
                f"qc_writer_last_batch_duration_seconds {self.last_batch_seconds:.3f}",
                "# TYPE qc_writer_last_batch_finished_timestamp_seconds gauge",
                f"qc_writer_last_batch_finished_timestamp_seconds {self.last_batch_finished_at:.0f}",
                "# TYPE qc_writer_dead_letters_total counter",
                f"qc_writer_dead_letters_total {self.dead_letters}",
                "# TYPE qc_writer_merge_retries_total counter",
                f"qc_writer_merge_retries_total {self.merge_retries}",
                "# TYPE qc_writer_schema_changes_total counter",
                f"qc_writer_schema_changes_total {self.schema_changes}",
                "# TYPE qc_writer_offsets_behind_latest gauge",
                f'qc_writer_offsets_behind_latest{{stat="max"}} {behind_max}',
                f'qc_writer_offsets_behind_latest{{stat="avg"}} {behind_avg}',
                "# TYPE qc_writer_events_total counter",
            ]
            lines += [
                f'qc_writer_events_total{{table="{t}",op="{o}"}} {n}'
                for (t, o), n in sorted(self.events.items())
            ]
        return "\n".join(lines) + "\n"

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "batches": self.batches,
                "last_batch_id": self.last_batch_id,
                "last_batch_seconds": self.last_batch_seconds,
                "last_batch_finished_at": self.last_batch_finished_at,
                "dead_letters": self.dead_letters,
                "merge_retries": self.merge_retries,
                "progress": self.progress,
            }


def serve(metrics: WriterMetrics, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/metrics":
                body, ctype = metrics.prometheus().encode(), "text/plain; version=0.0.4"
            elif self.path == "/progress":
                body, ctype = json.dumps(metrics.snapshot(), default=str).encode(), "application/json"
            elif self.path == "/healthz":
                body, ctype = b"ok\n", "text/plain"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: Any) -> None:  # keep the JSON log stream clean
            return

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)  # noqa: S104 - container-internal endpoint
    threading.Thread(target=server.serve_forever, name="metrics-http", daemon=True).start()
    return server
