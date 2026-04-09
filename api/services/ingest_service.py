import asyncio
import json
import logging
import os
import tempfile
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
from Modal.aggregator import aggregate
from Modal.worker import process_chunk_local

logger = logging.getLogger("api.services.ingest_service")
_executor = ThreadPoolExecutor(max_workers=8)

MAX_UPLOAD_BYTES = 50 * 1024 * 1024
WATCH_TIMEOUT_SECONDS = 300
WATCH_POLL_SECONDS = 2
STREAM_TIMEOUT_SECONDS = 300.0
STREAM_POLL_SECONDS = 0.5


def _client_ip(request: Request) -> str:
	return request.client.host if request.client else "unknown"


def _enforce_quota_or_raise(user_ip: str) -> int:
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
	if len(content) > MAX_UPLOAD_BYTES:
		raise HTTPException(status_code=413, detail="File too large. Max 50MB.")
	if content[:4] != b"%PDF":
		raise HTTPException(status_code=422, detail="File does not appear to be a PDF.")


def _write_temp_pdf(job_id: str, content: bytes) -> str:
	temp_path = os.path.join(tempfile.gettempdir(), f"{job_id}.pdf")
	with open(temp_path, "wb") as file_handle:
		file_handle.write(content)
	return temp_path


def _load_chunks_with_dedup(
	temp_path: str,
) -> tuple[str | None, dict[str, Any] | None, list[dict[str, Any]] | None]:
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
	if mode == "sequential":
		background_tasks.add_task(_run_sequential, job_id, chunks)
	else:
		background_tasks.add_task(_run_parallel, job_id, chunks)


async def _watch_and_aggregate(job_id: str, total_chunks: int) -> None:
	waited = 0
	loop = asyncio.get_running_loop()

	while waited < WATCH_TIMEOUT_SECONDS:
		await asyncio.sleep(WATCH_POLL_SECONDS)
		waited += WATCH_POLL_SECONDS
		job = get_job(job_id)
		if not job:
			return
		if job["done_chunks"] >= total_chunks:
			await loop.run_in_executor(_executor, aggregate, job_id)
			return

	logger.error("Timed out waiting for chunk completion for job %s", job_id)
	get_redis().hset(job_id, "status", "error")


async def _run_sequential(job_id: str, chunks: list[dict[str, Any]]) -> None:
	loop = asyncio.get_running_loop()
	for chunk in chunks:
		await loop.run_in_executor(_executor, process_chunk_local, job_id, chunk)


async def _run_parallel(job_id: str, chunks: list[dict[str, Any]]) -> None:
	loop = asyncio.get_running_loop()
	tasks = [
		loop.run_in_executor(_executor, process_chunk_local, job_id, chunk)
		for chunk in chunks
	]
	await asyncio.gather(*tasks)


async def ingest_pdf(
	request: Request,
	file: UploadFile,
	background_tasks: BackgroundTasks,
	mode: str = "parallel",
) -> dict[str, Any] | JSONResponse:
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
	_dispatch_chunk_tasks(background_tasks, mode, job_id, chunks)
	background_tasks.add_task(_watch_and_aggregate, job_id, len(chunks))

	return {
		"job_id": job_id,
		"total_chunks": len(chunks),
		"uploads_remaining": max(0, RATE_LIMIT_UPLOADS - used - 1),
		"cached": False,
	}


def get_status(job_id: str) -> dict[str, Any]:
	job = get_job(job_id)
	if not job:
		raise HTTPException(status_code=404, detail="Job not found")
	return _status_payload(job)


def get_result(job_id: str) -> dict[str, Any]:
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


def get_quota(request: Request) -> dict[str, int]:
	return get_rate_limit_status(_client_ip(request))


async def stream_events(job_id: str):
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