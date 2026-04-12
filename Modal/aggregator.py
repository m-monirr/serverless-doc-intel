"""Final aggregation pass."""

from typing import Any

from api.tracker import cache_result_by_md5, get_job, set_final_output


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
			if point and point not in all_points:
				all_points.append(point)

	if not all_points:
		all_points = [r.get("summary", "") for r in results if r.get("summary")]

	abstract = " ".join(r.get("summary", "") for r in results[:4]).strip()
	abstract = abstract[:1200] if abstract else "Document processed successfully."

	# 3) Build final shape expected by API consumers/front-end.
	final = {
		"abstract": abstract,
		"top_key_points": all_points[:10],
		"documentation": {
			"introduction": results[0].get("summary", "") if results else "",
			"methods": results[len(results) // 3].get("summary", "") if results else "",
			"findings": results[(2 * len(results)) // 3].get("summary", "") if results else "",
			"conclusion": results[-1].get("summary", "") if results else "",
		},
		"total_chunks": job["total_chunks"],
		"failed_chunks": max(0, job["total_chunks"] - len(results)),
	}

	set_final_output(job_id, final)
	file_md5 = job.get("file_md5")
	if file_md5:
		# 4) Cache final output by MD5 for fast duplicate-file responses.
		cache_result_by_md5(file_md5, final)
	return final
