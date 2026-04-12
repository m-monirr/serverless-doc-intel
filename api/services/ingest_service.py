import asyncio
import json
import logging
import os
import threading
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from uuid import uuid4

from fastapi import BackgroundTasks, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse

from api.chunker import extract_and_chunk
from api.tracker import (
	RATE_LIMIT_UPLOADS,
	calculate_file_md5,
	check_rate_limit,
	get_cached_result,
	get_job,
	get_rate_limit_status,
	get_redis,
	init_job,
	record_upload,
)
from Modal.aggregator import aggregate, render_markdown_report
from Modal.worker import get_worker_runtime_stats, process_chunk

# Service layer for ingest/status/result/quota/stream routes.
# This keeps API route handlers thin and focused on HTTP wiring.
logger = logging.getLogger("api.services.ingest_service")
_executor = ThreadPoolExecutor(max_workers=8)

MAX_UPLOAD_BYTES = 50 * 1024 * 1024
WATCH_TIMEOUT_SECONDS = 300
WATCH_POLL_SECONDS = 2
STREAM_TIMEOUT_SECONDS = 300.0
STREAM_POLL_SECONDS = 0.5

_metrics_lock = threading.Lock()
_job_started_at: dict[str, float] = {}
_service_metrics: dict[str, float | int] = {
	"jobs_started": 0,
	"jobs_completed": 0,
	"jobs_failed": 0,
	"jobs_timed_out": 0,
	"aggregate_failures": 0,
	"last_job_duration_seconds": 0.0,
}


def _incr_metric(name: str) -> None:
	with _metrics_lock:
		_service_metrics[name] = int(_service_metrics.get(name, 0)) + 1


def _mark_job_started(job_id: str) -> None:
	with _metrics_lock:
		_job_started_at[job_id] = time.time()
		_service_metrics["jobs_started"] = int(_service_metrics.get("jobs_started", 0)) + 1


def _mark_job_finished(job_id: str, success: bool) -> None:
	with _metrics_lock:
		started_at = _job_started_at.pop(job_id, None)
		if started_at is not None:
			_service_metrics["last_job_duration_seconds"] = round(
				time.time() - started_at,
				3,
			)
		if success:
			_service_metrics["jobs_completed"] = int(
				_service_metrics.get("jobs_completed", 0)
			) + 1
		else:
			_service_metrics["jobs_failed"] = int(_service_metrics.get("jobs_failed", 0)) + 1


def get_runtime_observability() -> dict[str, Any]:
	"""Return service and worker runtime counters for troubleshooting."""
	with _metrics_lock:
		service_counts = dict(_service_metrics)
		in_progress_jobs = len(_job_started_at)

	return {
		"service": {
			"counters": service_counts,
			"in_progress_jobs": in_progress_jobs,
		},
		"worker": get_worker_runtime_stats(),
	}


def _client_ip(request: Request) -> str:
	"""Extract caller IP for quota tracking."""
	return request.client.host if request.client else "unknown"


def _enforce_quota_or_raise(user_ip: str) -> int:
	"""Validate upload quota and return currently used uploads."""
	allowed, used = check_rate_limit(user_ip)
	if not allowed:
		status = get_rate_limit_status(user_ip)
		raise HTTPException(
			status_code=429,
			detail={
				"error": "Rate limit exceeded",
				"message": f"You can upload {RATE_LIMIT_UPLOADS} PDFs every 6 hours.",
				"resets_in_minutes": status["resets_in_minutes"],
			},
		)
	return used


def _validate_pdf_bytes(content: bytes) -> None:
	"""Fail fast on oversized payloads or non-PDF uploads."""
	if len(content) > MAX_UPLOAD_BYTES:
		raise HTTPException(status_code=413, detail="File too large. Max 50MB.")
	if content[:4] != b"%PDF":
		raise HTTPException(status_code=422, detail="File does not appear to be a PDF.")


def _write_temp_pdf(job_id: str, content: bytes) -> str:
	"""Persist uploaded bytes to a temporary file for OCR/hash steps."""
	temp_path = os.path.join(tempfile.gettempdir(), f"{job_id}.pdf")
	with open(temp_path, "wb") as file_handle:
		file_handle.write(content)
	return temp_path


def _load_chunks_with_dedup(
	temp_path: str,
) -> tuple[str | None, dict[str, Any] | None, list[dict[str, Any]] | None]:
	"""Return cached result for duplicate files, otherwise extracted chunks."""
	file_md5 = calculate_file_md5(temp_path)
	if file_md5:
		cached = get_cached_result(file_md5)
		if cached:
			return file_md5, cached, None

	chunks = extract_and_chunk(temp_path)
	if not chunks:
		raise HTTPException(status_code=422, detail="No chunks extracted from PDF.")
	return file_md5, None, chunks


def _status_payload(job: dict[str, Any]) -> dict[str, Any]:
	"""Normalize tracker job data into the public status response shape."""
	total = max(1, int(job["total_chunks"]))
	done = int(job["done_chunks"])
	return {
		"status": job["status"],
		"done_chunks": done,
		"total_chunks": int(job["total_chunks"]),
		"progress_pct": round(done / total * 100),
	}


def _dispatch_chunk_tasks(
	background_tasks: BackgroundTasks,
	mode: str,
	job_id: str,
	chunks: list[dict[str, Any]],
) -> None:
	"""Schedule chunk processing in sequential or parallel mode."""
	if mode == "sequential":
		background_tasks.add_task(_run_sequential, job_id, chunks)
	else:
		background_tasks.add_task(_run_parallel, job_id, chunks)


async def _watch_and_aggregate(job_id: str, total_chunks: int) -> None:
	"""Wait for chunk completion, then run final aggregation once."""
	waited = 0
	loop = asyncio.get_running_loop()

	while waited < WATCH_TIMEOUT_SECONDS:
		await asyncio.sleep(WATCH_POLL_SECONDS)
		waited += WATCH_POLL_SECONDS
		job = get_job(job_id)
		if not job:
			_mark_job_finished(job_id, success=False)
			return
		if job["done_chunks"] >= total_chunks:
			try:
				await loop.run_in_executor(_executor, aggregate, job_id)
				_mark_job_finished(job_id, success=True)
			except Exception:
				_incr_metric("aggregate_failures")
				_mark_job_finished(job_id, success=False)
				logger.exception("Aggregation failed for job %s", job_id)
				get_redis().hset(job_id, "status", "error")
			return

	logger.error("Timed out waiting for chunk completion for job %s", job_id)
	_incr_metric("jobs_timed_out")
	_mark_job_finished(job_id, success=False)
	get_redis().hset(job_id, "status", "error")


async def _run_sequential(job_id: str, chunks: list[dict[str, Any]]) -> None:
	"""Process chunks one-by-one (useful for easier debugging)."""
	loop = asyncio.get_running_loop()
	for chunk in chunks:
		await loop.run_in_executor(_executor, process_chunk, job_id, chunk)


async def _run_parallel(job_id: str, chunks: list[dict[str, Any]]) -> None:
	"""Process all chunks concurrently using the shared thread pool."""
	loop = asyncio.get_running_loop()
	tasks = [
		loop.run_in_executor(_executor, process_chunk, job_id, chunk)
		for chunk in chunks
	]
	await asyncio.gather(*tasks)


async def ingest_pdf(
	request: Request,
	file: UploadFile,
	background_tasks: BackgroundTasks,
	mode: str = "parallel",
) -> dict[str, Any] | JSONResponse:
	"""End-to-end ingest pipeline used by POST /ingest."""
	# Pipeline stages:
	# 1) Quota check
	# 2) Validate + temporarily persist upload
	# 3) Deduplicate by MD5 or extract chunks
	# 4) Initialize job state and schedule processing + aggregation watcher
	user_ip = _client_ip(request)
	used = _enforce_quota_or_raise(user_ip)
	job_id = str(uuid4())
	file_md5: str | None = None
	chunks: list[dict[str, Any]] | None = None
	temp_path: str | None = None

	try:
		content = await file.read()
		_validate_pdf_bytes(content)
		temp_path = _write_temp_pdf(job_id, content)
		file_md5, cached, chunks = _load_chunks_with_dedup(temp_path)
		if cached is not None:
			return JSONResponse(
				{
					"job_id": None,
					"cached": True,
					"result": cached,
					"message": "Identical PDF processed before. Returning cached result instantly.",
				}
			)
	except HTTPException:
		raise
	except ValueError as exc:
		raise HTTPException(status_code=422, detail=str(exc)) from exc
	finally:
		if temp_path and os.path.exists(temp_path):
			os.remove(temp_path)

	if chunks is None:
		raise HTTPException(status_code=500, detail="Chunk extraction failed unexpectedly.")

	record_upload(user_ip)
	init_job(job_id, len(chunks), file_md5=file_md5)
	_mark_job_started(job_id)
	_dispatch_chunk_tasks(background_tasks, mode, job_id, chunks)
	background_tasks.add_task(_watch_and_aggregate, job_id, len(chunks))

	return {
		"job_id": job_id,
		"total_chunks": len(chunks),
		"uploads_remaining": max(0, RATE_LIMIT_UPLOADS - used - 1),
		"cached": False,
	}


def get_status(job_id: str) -> dict[str, Any]:
	"""Public status accessor for GET /status/{job_id}."""
	job = get_job(job_id)
	if not job:
		raise HTTPException(status_code=404, detail="Job not found")
	return _status_payload(job)


def get_result(job_id: str) -> dict[str, Any]:
	"""Public result accessor for GET /result/{job_id}."""
	job = get_job(job_id)
	if not job:
		raise HTTPException(status_code=404, detail="Job not found")

	if job["status"] != "done":
		return {
			"error": "Not ready",
			"status": job["status"],
			"progress": f"{job['done_chunks']}/{job['total_chunks']}",
		}

	return json.loads(job.get("final_output", "{}"))


def get_result_markdown(job_id: str) -> str:
	"""Return markdown report for a completed job result."""
	job = get_job(job_id)
	if not job:
		raise HTTPException(status_code=404, detail="Job not found")

	if job["status"] != "done":
		raise HTTPException(status_code=409, detail="Result not ready")

	output = json.loads(job.get("final_output", "{}"))
	return render_markdown_report(output, job_id=job_id)


def get_quota(request: Request) -> dict[str, int]:
	"""Return quota counters for the current caller IP."""
	return get_rate_limit_status(_client_ip(request))


async def stream_events(job_id: str):
	"""Yield server-sent events for chunk updates and final completion."""
	seen: set[int] = set()
	waited = 0.0

	while waited < STREAM_TIMEOUT_SECONDS:
		job = get_job(job_id)
		if not job:
			yield f"data: {json.dumps({'event': 'error', 'detail': 'job not found'})}\n\n"
			break

		for result_item in job.get("results", []):
			chunk_id = int(result_item["chunk_id"])
			if chunk_id not in seen:
				seen.add(chunk_id)
				yield f"data: {json.dumps(result_item)}\n\n"

		if job["status"] == "done":
			final = json.loads(job.get("final_output", "{}"))
			yield f"data: {json.dumps({'event': 'done', 'output': final})}\n\n"
			break

		if job["status"] == "error":
			yield f"data: {json.dumps({'event': 'error', 'detail': 'processing failed'})}\n\n"
			break

		await asyncio.sleep(STREAM_POLL_SECONDS)
		waited += STREAM_POLL_SECONDS