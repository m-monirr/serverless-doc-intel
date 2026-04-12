"""Redis tracking, quota, and dedup helpers."""

import hashlib
import json
import os
import time
from typing import Any

import redis

_FAKE_REDIS = None

RATE_LIMIT_UPLOADS = int(os.getenv("RATE_LIMIT_UPLOADS", "2"))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", str(6 * 3600)))

JOB_TTL_SECONDS = 3600
MD5_CACHE_TTL_SECONDS = 86400


def get_redis() -> redis.Redis:
	"""Create Redis client and fall back to fakeredis for local development."""
	global _FAKE_REDIS

	redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
	client = redis.from_url(redis_url, decode_responses=True)
	try:
		client.ping()
		return client
	except Exception:
		if os.getenv("ALLOW_FAKE_REDIS", "1") != "1":
			raise

		import fakeredis

		if _FAKE_REDIS is None:
			_FAKE_REDIS = fakeredis.FakeRedis(decode_responses=True)
		return _FAKE_REDIS


def init_job(job_id: str, total_chunks: int, file_md5: str | None = None) -> None:
	"""Create job metadata and set TTL."""
	# Job hash stores status + counters used by /status and /result endpoints.
	r = get_redis()
	r.hset(
		job_id,
		mapping={
			"status": "processing",
			"total_chunks": total_chunks,
			"done_chunks": 0,
			"file_md5": file_md5 or "",
		},
	)
	r.expire(job_id, JOB_TTL_SECONDS)


def push_result(job_id: str, result: dict[str, Any]) -> int:
	"""Atomically append result and increment done counter."""
	# RPUSH + HINCRBY in one pipeline avoids race issues with concurrent workers.
	r = get_redis()
	pipe = r.pipeline()
	pipe.rpush(f"{job_id}:results", json.dumps(result))
	pipe.hincrby(job_id, "done_chunks", 1)
	_, new_count = pipe.execute()
	r.expire(f"{job_id}:results", JOB_TTL_SECONDS)
	return int(new_count)


def get_job(job_id: str) -> dict[str, Any] | None:
	"""Fetch job status and all chunk results."""
	r = get_redis()
	data = r.hgetall(job_id)
	if not data:
		return None

	raw_results = r.lrange(f"{job_id}:results", 0, -1)
	data["results"] = [json.loads(x) for x in raw_results]
	data["total_chunks"] = int(data.get("total_chunks", 0))
	data["done_chunks"] = int(data.get("done_chunks", 0))
	return data


def set_final_output(job_id: str, output: dict[str, Any]) -> None:
	"""Store final output and mark the job complete."""
	r = get_redis()
	r.hset(job_id, mapping={"status": "done", "final_output": json.dumps(output)})
	r.expire(job_id, JOB_TTL_SECONDS)


def _valid_upload_timestamps(entries: list[str]) -> list[float]:
	now = time.time()
	valid: list[float] = []
	for item in entries:
		try:
			ts = float(item)
		except ValueError:
			continue
		if now - ts < RATE_LIMIT_WINDOW:
			valid.append(ts)
	return valid


def check_rate_limit(user_ip: str) -> tuple[bool, int]:
	"""Return whether upload is allowed and currently used uploads."""
	# Store timestamps per IP and count only entries still inside the time window.
	r = get_redis()
	key = f"ratelimit:{user_ip}"
	entries = r.lrange(key, 0, -1)
	valid = _valid_upload_timestamps(entries)
	return len(valid) < RATE_LIMIT_UPLOADS, len(valid)


def record_upload(user_ip: str) -> None:
	"""Record a successful upload for this IP."""
	r = get_redis()
	key = f"ratelimit:{user_ip}"
	pipe = r.pipeline()
	pipe.rpush(key, str(time.time()))
	pipe.expire(key, RATE_LIMIT_WINDOW)
	pipe.execute()


def get_rate_limit_status(user_ip: str) -> dict[str, int]:
	"""Return current quota status with time-to-reset."""
	r = get_redis()
	key = f"ratelimit:{user_ip}"
	entries = r.lrange(key, 0, -1)
	valid = _valid_upload_timestamps(entries)

	reset_in = 0
	if valid:
		oldest = min(valid)
		reset_in = max(0, int(RATE_LIMIT_WINDOW - (time.time() - oldest)))

	return {
		"uploads_used": len(valid),
		"uploads_remaining": max(0, RATE_LIMIT_UPLOADS - len(valid)),
		"resets_in_seconds": reset_in,
		"resets_in_minutes": reset_in // 60,
	}


def calculate_file_md5(file_path: str) -> str | None:
	"""Compute MD5 hash of a file path."""
	hash_md5 = hashlib.md5()
	try:
		with open(file_path, "rb") as f:
			for chunk in iter(lambda: f.read(4096), b""):
				hash_md5.update(chunk)
		return hash_md5.hexdigest()
	except OSError:
		return None


def get_cached_result(file_md5: str) -> dict[str, Any] | None:
	"""Return cached final output for file MD5, if available."""
	r = get_redis()
	cached = r.get(f"md5cache:{file_md5}")
	return json.loads(cached) if cached else None


def cache_result_by_md5(file_md5: str, output: dict[str, Any]) -> None:
	"""Store final output by MD5 so duplicate uploads can return instantly."""
	# Cache TTL lets duplicate file uploads skip OCR + processing work.
	r = get_redis()
	r.set(f"md5cache:{file_md5}", json.dumps(output), ex=MD5_CACHE_TTL_SECONDS)
