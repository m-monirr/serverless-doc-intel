"""Streamlit frontend app."""

import time
from typing import Any

import requests
import streamlit as st

API_BASE = st.secrets.get("api_base", "http://localhost:8000")


def _get(path: str) -> dict[str, Any]:
	response = requests.get(f"{API_BASE}{path}", timeout=30)
	response.raise_for_status()
	return response.json()


def _post_file(path: str, file_bytes: bytes, file_name: str) -> dict[str, Any]:
	files = {"file": (file_name, file_bytes, "application/pdf")}
	response = requests.post(f"{API_BASE}{path}", files=files, timeout=120)
	if response.status_code >= 400:
		return {"error": response.text, "status_code": response.status_code}
	return response.json()


st.set_page_config(page_title="Async PDF Intelligence", page_icon="📄", layout="wide")
st.title("Async PDF Intelligence")
st.caption("Upload a PDF, monitor progress, and view structured analysis.")

uploaded = st.file_uploader("Upload PDF", type=["pdf"])
mode = st.selectbox("Processing mode", ["parallel", "sequential"], index=0)

if "job_id" not in st.session_state:
	st.session_state.job_id = None

if uploaded is not None and st.button("Start Analysis", type="primary"):
	if uploaded.size > 50 * 1024 * 1024:
		st.error("File too large. Max 50MB.")
	else:
		payload = _post_file(f"/ingest?mode={mode}", uploaded.getvalue(), uploaded.name)
		if payload.get("cached"):
			st.success("Cached result returned instantly.")
			st.json(payload.get("result", {}))
		elif payload.get("job_id"):
			st.session_state.job_id = payload["job_id"]
			st.success(f"Job started: {payload['job_id']}")
		else:
			st.error(f"Upload failed: {payload}")

if st.session_state.job_id:
	job_id = st.session_state.job_id
	st.subheader("Progress")
	progress = st.progress(0)
	status_box = st.empty()

	for _ in range(240):
		status = _get(f"/status/{job_id}")
		progress.progress(int(status.get("progress_pct", 0)))
		status_box.write(status)
		if status.get("status") in {"done", "error"}:
			break
		time.sleep(1)

	result = _get(f"/result/{job_id}")
	st.subheader("Result")
	if result.get("error") == "Not ready":
		st.warning("Result not ready yet. Keep polling status.")
	else:
		st.markdown("### Abstract")
		st.write(result.get("abstract", ""))

		st.markdown("### Top Key Points")
		for point in result.get("top_key_points", []):
			st.write(f"- {point}")

		docs = result.get("documentation", {})
		st.markdown("### Documentation")
		for section in ["introduction", "methods", "findings", "conclusion"]:
			with st.expander(section.capitalize(), expanded=(section == "introduction")):
				st.write(docs.get(section, ""))

st.subheader("Quota")
try:
	st.json(_get("/quota"))
except Exception as exc:
	st.info(f"Quota unavailable: {exc}")
