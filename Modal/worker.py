"""Chunk worker logic.

This local implementation keeps project flow working without cloud workers.
It can be replaced later with Modal/Beam dispatch wrappers.
"""

import os
import threading
from typing import Any

import modal

from api.tracker import push_result

app = modal.App("pdf-pipeline-worker")


def _is_enabled(value: str | None, default: bool = False) -> bool:
	if value is None:
		return default
	return value.strip().lower() in {"1", "true", "yes", "on"}


USE_MODAL_REMOTE = _is_enabled(os.getenv("USE_MODAL_REMOTE"), default=False)
MODAL_LOCAL_FALLBACK = _is_enabled(os.getenv("MODAL_LOCAL_FALLBACK"), default=True)
MODAL_WORKER_APP = os.getenv("MODAL_WORKER_APP", "pdf-pipeline-worker")
MODAL_WORKER_FUNCTION = os.getenv("MODAL_WORKER_FUNCTION", "process_chunk_remote")

_metrics_lock = threading.Lock()
_worker_metrics: dict[str, int] = {
	"remote_success": 0,
	"remote_failure": 0,
	"fallback_used": 0,
	"local_success": 0,
}


def _incr_metric(name: str) -> None:
	with _metrics_lock:
		_worker_metrics[name] = _worker_metrics.get(name, 0) + 1


def get_worker_runtime_stats() -> dict[str, Any]:
	"""Return runtime worker mode and counters for observability."""
	with _metrics_lock:
		counts = dict(_worker_metrics)

	return {
		"mode": {
			"use_modal_remote": USE_MODAL_REMOTE,
			"modal_local_fallback": MODAL_LOCAL_FALLBACK,
			"modal_worker_app": MODAL_WORKER_APP,
			"modal_worker_function": MODAL_WORKER_FUNCTION,
		},
		"counters": counts,
	}


def _summarize_text(text: str) -> tuple[str, list[str], int]:
	# Lightweight local summarization fallback used when cloud workers are not active.
	clean = " ".join(text.split())
	if not clean:
		return "No text extracted in this section.", [], 1

	preview = clean[:280]
	if len(clean) > 280:
		preview += "..."

	points: list[str] = []
	for sentence in clean.replace("?", ".").replace("!", ".").split("."):
		s = sentence.strip()
		if len(s) >= 24:
			points.append(s)
		if len(points) == 3:
			break

	score = 1
	if len(clean) > 500:
		score = 2
	if len(clean) > 1200:
		score = 3
	if len(clean) > 2200:
		score = 4
	if len(clean) > 3200:
		score = 5

	return preview, points, score


def _build_result(chunk: dict[str, Any]) -> dict[str, Any]:
	"""Create normalized chunk output payload from raw chunk text."""
	chunk_id = int(chunk["chunk_id"])
	text = str(chunk.get("text", ""))

	summary, points, score = _summarize_text(text)
	return {
		"chunk_id": chunk_id,
		"summary": summary,
		"key_points": points,
		"importance_score": score,
	}


def process_chunk_local(job_id: str, chunk: dict[str, Any]) -> dict[str, Any]:
	"""Process one chunk and write result to tracker storage."""
	# This writes one per-chunk result and increments done_chunks in Redis.
	result = _build_result(chunk)
	push_result(job_id, result)
	_incr_metric("local_success")
	return result


def _call_modal_remote(job_id: str, chunk: dict[str, Any]) -> dict[str, Any]:
	"""Call deployed Modal worker function and store the returned result."""
	remote_fn = modal.Function.from_name(MODAL_WORKER_APP, MODAL_WORKER_FUNCTION)
	result = remote_fn.remote(job_id, chunk)
	if not isinstance(result, dict):
		raise RuntimeError("Modal worker returned non-dict result")
	push_result(job_id, result)
	return result


def process_chunk(job_id: str, chunk: dict[str, Any]) -> dict[str, Any]:
	"""Process one chunk via Modal remote worker, or local fallback."""
	if USE_MODAL_REMOTE:
		try:
			result = _call_modal_remote(job_id, chunk)
			_incr_metric("remote_success")
			return result
		except Exception:
			_incr_metric("remote_failure")
			if not MODAL_LOCAL_FALLBACK:
				raise
			_incr_metric("fallback_used")

	return process_chunk_local(job_id, chunk)


@app.function()
def process_chunk_remote(job_id: str, chunk: dict[str, Any]) -> dict[str, Any]:
	"""Modal-deployed worker function that computes one chunk result payload."""
	# Do not write to Redis here. The API process persists returned payload centrally.
	_ = job_id
	return _build_result(chunk)
