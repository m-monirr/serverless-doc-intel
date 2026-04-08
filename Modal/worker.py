"""Chunk worker logic.

This local implementation keeps project flow working without cloud workers.
It can be replaced later with Modal/Beam dispatch wrappers.
"""

from typing import Any

from api.tracker import push_result


def _summarize_text(text: str) -> tuple[str, list[str], int]:
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


def process_chunk_local(job_id: str, chunk: dict[str, Any]) -> dict[str, Any]:
	"""Process one chunk and write result to tracker storage."""
	chunk_id = int(chunk["chunk_id"])
	text = str(chunk.get("text", ""))

	summary, points, score = _summarize_text(text)
	result = {
		"chunk_id": chunk_id,
		"summary": summary,
		"key_points": points,
		"importance_score": score,
	}
	push_result(job_id, result)
	return result


def process_chunk(job_id: str, chunk: dict[str, Any]) -> dict[str, Any]:
	"""Compatibility wrapper for future remote worker adapters."""
	return process_chunk_local(job_id, chunk)
