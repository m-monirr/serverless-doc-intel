import asyncio
import json
import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from api.chunker import extract_and_chunk
from api.tracker import (
	RATE_LIMIT_UPLOADS,
	cache_result_by_md5,
	calculate_file_md5,
	check_rate_limit,
	get_cached_result,
	get_job,
	get_rate_limit_status,
	init_job,
	push_result,
	record_upload,
)
from Modal.aggregator import aggregate
from Modal.worker import process_chunk_local

app = FastAPI(title="Async PDF Pipeline", version="0.1.0")
logger = logging.getLogger("api.main")
_executor = ThreadPoolExecutor(max_workers=8)


async def _watch_and_aggregate(job_id: str, total_chunks: int) -> None:
	"""Wait for all chunk tasks then aggregate into final output."""
	# Poll job state until all chunk processors finish, then run one final
	# aggregation pass in a thread so the event loop stays responsive.
	waited = 0
	max_wait_seconds = 300
	loop = asyncio.get_running_loop()

	while waited < max_wait_seconds:
		await asyncio.sleep(2)
		waited += 2
		job = get_job(job_id)
		if not job:
			return
		if job["done_chunks"] >= total_chunks:
			await loop.run_in_executor(_executor, aggregate, job_id)
			return

	from api.tracker import get_redis

	get_redis().hset(job_id, "status", "error")


async def _run_sequential(job_id: str, chunks: list[dict[str, Any]]) -> None:
	# Useful for debugging and benchmarking against parallel mode.
	loop = asyncio.get_running_loop()
	for chunk in chunks:
		await loop.run_in_executor(_executor, process_chunk_local, job_id, chunk)


async def _run_parallel(job_id: str, chunks: list[dict[str, Any]]) -> None:
	# Fan out per-chunk work concurrently using thread pool workers.
	loop = asyncio.get_running_loop()
	tasks = [
		loop.run_in_executor(_executor, process_chunk_local, job_id, chunk)
		for chunk in chunks
	]
	await asyncio.gather(*tasks)


@app.get("/health")
def health() -> dict[str, str]:
	return {"status": "ok"}


@app.post("/ingest")
async def ingest(
	request: Request,
	file: UploadFile,
	background_tasks: BackgroundTasks,
	mode: str = "parallel",
):
	# 1) Enforce quota by client IP before doing any heavy work.
	user_ip = request.client.host if request.client else "unknown"
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

	job_id = str(uuid4())
	temp_path = os.path.join(tempfile.gettempdir(), f"{job_id}.pdf")
	file_md5: str | None = None

	try:
		# 2) Read and validate uploaded bytes.
		content = await file.read()
		if len(content) > 50 * 1024 * 1024:
			raise HTTPException(status_code=413, detail="File too large. Max 50MB.")
		if content[:4] != b"%PDF":
			raise HTTPException(status_code=422, detail="File does not appear to be a PDF.")

		# 3) Save temporarily for hashing and OCR/chunk extraction.
		with open(temp_path, "wb") as f:
			f.write(content)

		# 4) Deduplicate by file hash to return cached output instantly.
		file_md5 = calculate_file_md5(temp_path)
		if file_md5:
			cached = get_cached_result(file_md5)
			if cached:
				return JSONResponse(
					{
						"job_id": None,
						"cached": True,
						"result": cached,
						"message": "Identical PDF processed before. Returning cached result instantly.",
					}
				)

		# 5) Convert PDF to markdown and split into chunks.
		chunks = extract_and_chunk(temp_path)
		if not chunks:
			raise HTTPException(status_code=422, detail="No chunks extracted from PDF.")

	except ValueError as exc:
		raise HTTPException(status_code=422, detail=str(exc)) from exc
	finally:
		if os.path.exists(temp_path):
			os.remove(temp_path)

	# 6) Initialize job state and dispatch chunk workers.
	record_upload(user_ip)
	init_job(job_id, len(chunks), file_md5=file_md5)

	if mode == "sequential":
		background_tasks.add_task(_run_sequential, job_id, chunks)
	else:
		background_tasks.add_task(_run_parallel, job_id, chunks)

	# 7) Start watcher that will run final aggregation when chunks are done.
	background_tasks.add_task(_watch_and_aggregate, job_id, len(chunks))

	return {
		"job_id": job_id,
		"total_chunks": len(chunks),
		"uploads_remaining": max(0, RATE_LIMIT_UPLOADS - used - 1),
		"cached": False,
	}


@app.get("/status/{job_id}")
def status(job_id: str) -> dict[str, Any]:
	job = get_job(job_id)
	if not job:
		raise HTTPException(status_code=404, detail="Job not found")

	total = max(1, job["total_chunks"])
	return {
		"status": job["status"],
		"done_chunks": job["done_chunks"],
		"total_chunks": job["total_chunks"],
		"progress_pct": round(job["done_chunks"] / total * 100),
	}


@app.get("/result/{job_id}")
def result(job_id: str) -> dict[str, Any]:
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


@app.get("/quota")
def quota(request: Request) -> dict[str, int]:
	user_ip = request.client.host if request.client else "unknown"
	return get_rate_limit_status(user_ip)


@app.get("/stream/{job_id}")
async def stream(job_id: str) -> StreamingResponse:
	"""Server-sent events stream with chunk updates and final output."""

	async def event_generator():
		seen: set[int] = set()
		waited = 0.0
		timeout = 300.0

		while waited < timeout:
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

			await asyncio.sleep(0.5)
			waited += 0.5

	return StreamingResponse(event_generator(), media_type="text/event-stream")
