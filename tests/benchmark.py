"""Pipeline benchmark script."""

import time

import requests

BASE_URL = "http://localhost:8000"
PDF_PATH = "tests/sample.pdf"


def poll_until_done(job_id: str, max_wait: int = 180) -> dict:
	start = time.time()
	while time.time() - start < max_wait:
		payload = requests.get(f"{BASE_URL}/status/{job_id}", timeout=30).json()
		if payload["status"] == "done":
			return payload
		if payload["status"] == "error":
			raise RuntimeError(f"Job {job_id} failed")
		time.sleep(0.5)
	raise TimeoutError(f"Job {job_id} did not finish within {max_wait}s")


def run_mode(mode: str) -> float:
	t0 = time.time()
	with open(PDF_PATH, "rb") as f:
		response = requests.post(
			f"{BASE_URL}/ingest?mode={mode}",
			files={"file": ("sample.pdf", f, "application/pdf")},
			timeout=120,
		)
	response.raise_for_status()
	job_id = response.json().get("job_id")
	if not job_id:
		raise RuntimeError(f"No job_id returned for mode={mode}: {response.text}")
	poll_until_done(job_id)
	return round(time.time() - t0, 2)


def benchmark() -> None:
	print("Benchmark starting...\n")

	print("Running: parallel")
	parallel_t = run_mode("parallel")
	print(f"  {parallel_t}s\n")

	time.sleep(2)

	print("Running: sequential")
	sequential_t = run_mode("sequential")
	print(f"  {sequential_t}s\n")

	speedup = sequential_t / parallel_t if parallel_t > 0 else 0.0
	print("=" * 44)
	print(f"  Parallel:   {parallel_t}s")
	print(f"  Sequential: {sequential_t}s")
	print(f"  Speedup:    {speedup:.1f}x")
	print("=" * 44)


if __name__ == "__main__":
	benchmark()
