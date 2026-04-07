import asyncio
import json
import logging
import os
import tempfile
from typing import Any
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse

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
	set_final_output,
)

app = FastAPI(title="Async PDF Pipeline", version="0.1.0")
logger = logging.getLogger("api.main")


def _placeholder_chunk_result(chunk: dict[str, Any]) -> dict[str, Any]:
	text = str(chunk["text"]).strip()
	words = text.split()
	preview = " ".join(words[:40]).strip()
	return {
		"chunk_id": int(chunk["chunk_id"]),
		"summary": preview if preview else "[No text extracted in this chunk]",
		"key_points": [],
		"importance_score": 1,
	}


def process_chunk_local(job_id: str, chunk: dict[str, Any]) -> None:
	"""Phase 1 local placeholder processor (no Beam, no LLM)."""
	result = _placeholder_chunk_result(chunk)
	push_result(job_id, result)


def _build_placeholder_final(job: dict[str, Any]) -> dict[str, Any]:
	summaries = [r.get("summary", "") for r in job["results"]]
	top_points = [s for s in summaries if s][:10]
	return {
		"abstract": "Placeholder output for Phase 1. Beam + LLM integration not enabled yet.",
		"top_key_points": top_points,
		"documentation": {
			"introduction": "Phase 1 foundation run.",
			"methods": "Docling OCR and local chunk placeholder processing.",
			"findings": "Chunk summaries were generated without LLM inference.",
			"conclusion": "Pipeline plumbing is working and ready for Beam integration.",
		},
		"total_chunks": job["total_chunks"],
		"failed_chunks": max(0, job["total_chunks"] - len(job["results"])),
	}


async def _watch_and_finalize(job_id: str, total_chunks: int, file_md5: str | None) -> None:
	"""Phase 1 watcher stub: waits for completion and stores placeholder final output."""
	waited = 0
	max_wait_seconds = 300

	while waited < max_wait_seconds:
		await asyncio.sleep(2)
		waited += 2
		job = get_job(job_id)
		if not job:
			return
		if job["done_chunks"] >= total_chunks:
			logger.info("job %s ready to aggregate (Phase 1 stub)", job_id)
			final_output = _build_placeholder_final(job)
			set_final_output(job_id, final_output)
			if file_md5:
				cache_result_by_md5(file_md5, final_output)
			return

	set_final_output(
		job_id,
		{
			"abstract": "Processing timed out.",
			"top_key_points": [],
			"documentation": {},
			"total_chunks": total_chunks,
			"failed_chunks": total_chunks,
			"error": "timeout",
		},
	)


@app.get("/health")
def health() -> dict[str, str]:
	return {"status": "ok"}


@app.post("/ingest")
async def ingest(request: Request, file: UploadFile, background_tasks: BackgroundTasks):
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
		content = await file.read()
		if len(content) > 50 * 1024 * 1024:
			raise HTTPException(status_code=413, detail="File too large. Max 50MB.")
		if content[:4] != b"%PDF":
			raise HTTPException(status_code=422, detail="File does not appear to be a PDF.")

		with open(temp_path, "wb") as f:
			f.write(content)

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

		chunks = extract_and_chunk(temp_path)
		if not chunks:
			raise HTTPException(status_code=422, detail="No chunks extracted from PDF.")

	except ValueError as exc:
		raise HTTPException(status_code=422, detail=str(exc)) from exc
	finally:
		if os.path.exists(temp_path):
			os.remove(temp_path)

	record_upload(user_ip)
	init_job(job_id, len(chunks), file_md5=file_md5)

	for chunk in chunks:
		process_chunk_local(job_id, chunk)

	background_tasks.add_task(_watch_and_finalize, job_id, len(chunks), file_md5)

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
