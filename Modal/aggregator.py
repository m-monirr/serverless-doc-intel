"""Final aggregation pass."""

import json
import re
from typing import Any

from Modal.llm_client import (
	ALLOW_LLM_FALLBACK,
	USE_LLM_ANALYSIS,
	call_vllm_prompt,
	parse_json_object,
)
from api.tracker import cache_result_by_md5, get_job, set_final_output


def _clean_text(value: str) -> str:
	text = " ".join(str(value).split())
	text = re.sub(r"<!--\s*image\s*-->", " ", text, flags=re.IGNORECASE)
	text = text.replace("```", " ")
	return " ".join(text.split()).strip()


def _is_noisy(value: str) -> bool:
	text = _clean_text(value)
	if len(text) < 24:
		return True
	alnum = sum(ch.isalnum() for ch in text)
	if alnum / max(1, len(text)) < 0.4:
		return True
	return False


def render_markdown_report(final_output: dict[str, Any], job_id: str | None = None) -> str:
	"""Render final JSON output into a human-friendly markdown report."""
	heading = "# PDF Review Report"
	if job_id:
		heading += f"\n\nJob ID: {job_id}"

	abstract = str(final_output.get("abstract", "")).strip() or "No abstract available."
	points = final_output.get("top_key_points", [])
	if not isinstance(points, list):
		points = []

	docs = final_output.get("documentation", {})
	if not isinstance(docs, dict):
		docs = {}

	lines: list[str] = [
		heading,
		"",
		"## Abstract",
		"",
		abstract,
		"",
		"## Top Key Points",
		"",
	]

	if points:
		for point in points:
			lines.append(f"- {point}")
	else:
		lines.append("- No key points available.")

	lines.extend(
		[
			"",
			"## Documentation",
			"",
			"### Introduction",
			str(docs.get("introduction", "")),
			"",
			"### Methods",
			str(docs.get("methods", "")),
			"",
			"### Findings",
			str(docs.get("findings", "")),
			"",
			"### Conclusion",
			str(docs.get("conclusion", "")),
			"",
			"## Metrics",
			"",
			f"- Total chunks: {int(final_output.get('total_chunks', 0))}",
			f"- Failed chunks: {int(final_output.get('failed_chunks', 0))}",
		]
	)

	return "\n".join(lines) + "\n"


def aggregate(job_id: str) -> dict[str, Any]:
	"""Build final output from chunk-level results and persist it."""
	# 1) Read all chunk outputs from tracker storage.
	job = get_job(job_id)
	if not job:
		raise ValueError(f"Job {job_id} not found")

	results = sorted(job.get("results", []), key=lambda item: int(item["chunk_id"]))
	selected_chunk_ids: set[int] = set()
	raw_selected = job.get("selected_chunk_ids", "")
	if isinstance(raw_selected, str) and raw_selected:
		try:
			parsed = json.loads(raw_selected)
			if isinstance(parsed, list):
				selected_chunk_ids = {int(x) for x in parsed}
		except Exception:
			selected_chunk_ids = set()
	if not results:
		# Handle empty extraction case gracefully.
		final = {
			"abstract": "No content could be extracted.",
			"top_key_points": [],
			"documentation": {
				"introduction": "No content available.",
				"methods": "",
				"findings": "",
				"conclusion": "",
			},
			"total_chunks": job["total_chunks"],
			"failed_chunks": job["total_chunks"],
		}
		set_final_output(job_id, final)
		return final

	all_points: list[str] = []
	# 2) Merge key points across chunks with de-duplication.
	for r in results:
		for point in r.get("key_points", []):
			clean_point = _clean_text(point)
			if clean_point and not _is_noisy(clean_point) and clean_point not in all_points:
				all_points.append(clean_point)

	if not all_points:
		all_points = [
			_clean_text(r.get("summary", ""))
			for r in results
			if r.get("summary") and not _is_noisy(str(r.get("summary", "")))
		]

	clean_results: list[dict[str, Any]] = []
	for item in results:
		summary = _clean_text(str(item.get("summary", "")))
		if _is_noisy(summary):
			continue
		points = [
			_clean_text(str(p))
			for p in item.get("key_points", [])
			if not _is_noisy(str(p))
		]
		clean_results.append(
			{
				"chunk_id": int(item.get("chunk_id", 0)),
				"summary": summary,
				"key_points": points,
				"importance_score": int(item.get("importance_score", 3)),
			}
		)

	if clean_results:
		results = clean_results

	def _fallback_final() -> dict[str, Any]:
		abstract = " ".join(_clean_text(r.get("summary", "")) for r in results[:4]).strip()
		abstract_local = abstract[:1200] if abstract else "Document processed successfully."
		return {
			"abstract": abstract_local,
			"top_key_points": all_points[:10],
			"documentation": {
				"introduction": _clean_text(results[0].get("summary", "")) if results else "",
				"methods": _clean_text(results[len(results) // 3].get("summary", "")) if results else "",
				"findings": _clean_text(results[(2 * len(results)) // 3].get("summary", "")) if results else "",
				"conclusion": _clean_text(results[-1].get("summary", "")) if results else "",
			},
			"total_chunks": job["total_chunks"],
			"failed_chunks": max(0, job["total_chunks"] - len(results)),
		}

	if USE_LLM_ANALYSIS:
		try:
			llm_source_results = results
			if selected_chunk_ids:
				filtered = [r for r in results if int(r.get("chunk_id", -1)) in selected_chunk_ids]
				if filtered:
					llm_source_results = filtered

			payload = {
				"chunk_count": len(llm_source_results),
				"selected_chunk_ids": sorted(selected_chunk_ids),
				"chunks": [
					{
						"chunk_id": int(r.get("chunk_id", 0)),
						"summary": _clean_text(str(r.get("summary", ""))),
						"key_points": [
							_clean_text(str(x))
							for x in r.get("key_points", [])
							if not _is_noisy(str(x))
						],
						"importance_score": int(r.get("importance_score", 3)),
					}
					for r in llm_source_results
				],
			}
			prompt = (
				"Create one final integrated review from the JSON chunk analyses below. "
				"Ignore OCR artifacts, broken markup, XML snippets, and image placeholders. "
				"Write concise and readable prose. "
				"Return ONLY valid JSON with keys: abstract (string), top_key_points (array of strings), "
				"documentation (object with introduction, methods, findings, conclusion strings).\n\n"
				f"chunk_analyses_json:\n{json.dumps(payload)}"
			)
			llm_text = call_vllm_prompt(prompt, max_tokens=900, temperature=0.1)
			obj = parse_json_object(llm_text)
			doc = obj.get("documentation", {})
			if not isinstance(doc, dict):
				doc = {}
			final = {
				"abstract": str(obj.get("abstract", "")).strip() or "Document processed successfully.",
				"top_key_points": [str(x).strip() for x in obj.get("top_key_points", []) if str(x).strip()][:10],
				"documentation": {
					"introduction": str(doc.get("introduction", "")),
					"methods": str(doc.get("methods", "")),
					"findings": str(doc.get("findings", "")),
					"conclusion": str(doc.get("conclusion", "")),
				},
				"total_chunks": job["total_chunks"],
				"failed_chunks": max(0, job["total_chunks"] - len(results)),
			}
		except Exception:
			if not ALLOW_LLM_FALLBACK:
				raise
			final = _fallback_final()
	else:
		final = _fallback_final()

	set_final_output(job_id, final)
	file_md5 = job.get("file_md5")
	if file_md5:
		# 4) Cache final output by MD5 for fast duplicate-file responses.
		cache_result_by_md5(file_md5, final)
	return final
