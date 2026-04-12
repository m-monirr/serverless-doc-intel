"""Streamlit frontend app."""

import json
import os
import time
from typing import Any

import requests
import streamlit as st


def _result_to_markdown(result: dict[str, Any]) -> str:
	abstract = str(result.get("abstract", "")).strip() or "No abstract available."
	points = result.get("top_key_points", [])
	if not isinstance(points, list):
		points = []

	docs = result.get("documentation", {})
	if not isinstance(docs, dict):
		docs = {}

	lines: list[str] = [
		"# PDF Review Report",
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
			f"- Total chunks: {int(result.get('total_chunks', 0))}",
			f"- Failed chunks: {int(result.get('failed_chunks', 0))}",
		]
	)

	return "\n".join(lines) + "\n"


def _resolve_api_base() -> str:
	try:
		if "api_base" in st.secrets:
			return str(st.secrets["api_base"]).rstrip("/")
	except Exception:
		pass
	return os.getenv("API_BASE_URL", "http://localhost:8010").rstrip("/")


API_BASE = _resolve_api_base()


def _parse_json(response: requests.Response) -> dict[str, Any]:
	try:
		return response.json()
	except json.JSONDecodeError:
		return {"raw": response.text}


def _get(path: str, timeout: int = 30) -> tuple[bool, dict[str, Any]]:
	try:
		response = requests.get(f"{API_BASE}{path}", timeout=timeout)
		payload = _parse_json(response)
		if response.status_code >= 400:
			return False, {"status_code": response.status_code, "payload": payload}
		return True, payload
	except requests.RequestException as exc:
		return False, {"error": str(exc)}


def _post_file(path: str, file_bytes: bytes, file_name: str) -> tuple[bool, dict[str, Any]]:
	files = {"file": (file_name, file_bytes, "application/pdf")}
	try:
		response = requests.post(f"{API_BASE}{path}", files=files, timeout=1200)
		payload = _parse_json(response)
		if response.status_code >= 400:
			return False, {"status_code": response.status_code, "payload": payload}
		return True, payload
	except requests.RequestException as exc:
		return False, {"error": str(exc)}


def _get_text(path: str, timeout: int = 60) -> tuple[bool, str | dict[str, Any]]:
	try:
		response = requests.get(f"{API_BASE}{path}", timeout=timeout)
		if response.status_code >= 400:
			return False, {"status_code": response.status_code, "payload": _parse_json(response)}
		return True, response.text
	except requests.RequestException as exc:
		return False, {"error": str(exc)}


st.set_page_config(page_title="Async PDF Intelligence", page_icon="PDF", layout="wide")
st.title("Async PDF Intelligence")
st.caption("Upload PDF files and receive a structured review with progress tracking.")

with st.sidebar:
	st.subheader("Settings")
	st.code(API_BASE, language="text")
	mode = st.selectbox("Processing mode", ["parallel", "sequential"], index=0)
	poll_interval = st.slider("Poll interval (seconds)", min_value=1, max_value=5, value=2)
	auto_refresh = st.checkbox("Auto-refresh while running", value=True)

if "job_id" not in st.session_state:
	st.session_state.job_id = None
if "report_markdown" not in st.session_state:
	st.session_state.report_markdown = None

left, right = st.columns([2, 1])

with left:
	uploaded = st.file_uploader("Upload PDF", type=["pdf"])
	start_clicked = st.button("Start Analysis", type="primary", use_container_width=True)

if start_clicked:
	if uploaded is None:
		st.error("Please select a PDF file first.")
	elif uploaded.size > 50 * 1024 * 1024:
		st.error("File too large. Maximum size is 50MB.")
	else:
		with st.spinner("Analyzing file... this can take up to 1-2 minutes for OCR PDFs."):
			ok, payload = _post_file(f"/ingest?mode={mode}", uploaded.getvalue(), uploaded.name)
		if not ok:
			status_code = payload.get("status_code")
			details = payload.get("payload", payload)
			st.error(f"Upload failed ({status_code}): {details}")
		elif payload.get("cached"):
			st.success("Cached report returned instantly.")
			st.session_state.job_id = None
			st.session_state.report_markdown = payload.get("markdown_report")
			if not st.session_state.report_markdown:
				st.session_state.report_markdown = _result_to_markdown(payload.get("result", {}))
		else:
			st.session_state.job_id = payload.get("job_id")
			st.session_state.report_markdown = None
			st.success(f"Job started: {st.session_state.job_id}")

if st.session_state.job_id:
	st.subheader("Progress")

	ok, status_payload = _get(f"/status/{st.session_state.job_id}")
	if not ok:
		st.error(f"Could not fetch status: {status_payload}")
	else:
		pct = int(status_payload.get("progress_pct", 0))
		st.progress(max(0, min(100, pct)))

		s1, s2, s3 = st.columns(3)
		s1.metric("Status", str(status_payload.get("status", "unknown")).upper())
		s2.metric("Done", int(status_payload.get("done_chunks", 0)))
		s3.metric("Total", int(status_payload.get("total_chunks", 0)))

		if status_payload.get("status") == "done":
			ok, markdown_payload = _get_text(f"/result/{st.session_state.job_id}/markdown")
			if ok and isinstance(markdown_payload, str):
				st.session_state.report_markdown = markdown_payload
				st.session_state.job_id = None
			else:
				st.warning("Job finished but markdown report is not ready yet. Click refresh.")
		elif status_payload.get("status") == "error":
			st.error("Processing failed. Try uploading again.")
			st.session_state.job_id = None
		elif auto_refresh:
			time.sleep(poll_interval)
			st.rerun()

if st.session_state.report_markdown:
	st.success("Analysis complete. Download your markdown report.")
	st.download_button(
		label="Download Report (.md)",
		data=st.session_state.report_markdown,
		file_name="pdf-review-report.md",
		mime="text/markdown",
		use_container_width=True,
	)

with right:
	st.subheader("Quota")
	ok, quota_payload = _get("/quota")
	if ok:
		st.metric("Used", int(quota_payload.get("uploads_used", 0)))
		st.metric("Remaining", int(quota_payload.get("uploads_remaining", 0)))
		st.metric("Reset (minutes)", int(quota_payload.get("resets_in_minutes", 0)))
	else:
		st.info(f"Quota unavailable: {quota_payload}")

	if st.button("Refresh Status", use_container_width=True):
		st.rerun()
