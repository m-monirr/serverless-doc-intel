import io
import json
import unittest
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.testclient import TestClient

from api.main import app


class IngestServiceFlowTests(unittest.TestCase):
	def setUp(self) -> None:
		self.client = TestClient(app)

	def test_ingest_returns_429_when_rate_limited(self) -> None:
		with patch(
			"api.services.ingest_service._enforce_quota_or_raise",
			side_effect=HTTPException(status_code=429, detail={"error": "Rate limit exceeded"}),
		):
			files = {"file": ("sample.pdf", io.BytesIO(b"%PDF-1.4 fake"), "application/pdf")}
			response = self.client.post("/ingest", files=files)

		self.assertEqual(response.status_code, 429)
		self.assertIn("Rate limit exceeded", response.text)

	def test_ingest_returns_cached_result_when_md5_hit(self) -> None:
		with patch("api.services.ingest_service._enforce_quota_or_raise", return_value=0), patch(
			"api.services.ingest_service._write_temp_pdf", return_value="temp.pdf"
		), patch(
			"api.services.ingest_service._load_chunks_with_dedup",
			return_value=("abc123", {"abstract": "cached"}, None),
		), patch("api.services.ingest_service.os.path.exists", return_value=False):
			files = {"file": ("sample.pdf", io.BytesIO(b"%PDF-1.4 fake"), "application/pdf")}
			response = self.client.post("/ingest", files=files)

		self.assertEqual(response.status_code, 200)
		payload = response.json()
		self.assertTrue(payload["cached"])
		self.assertIsNone(payload["job_id"])
		self.assertEqual(payload["result"]["abstract"], "cached")

	def test_status_progress_payload(self) -> None:
		with patch(
			"api.services.ingest_service.get_job",
			return_value={"status": "processing", "done_chunks": 1, "total_chunks": 4},
		):
			response = self.client.get("/status/job-1")

		self.assertEqual(response.status_code, 200)
		self.assertEqual(response.json()["progress_pct"], 25)

	def test_result_not_ready_payload(self) -> None:
		with patch(
			"api.services.ingest_service.get_job",
			return_value={"status": "processing", "done_chunks": 2, "total_chunks": 5},
		):
			response = self.client.get("/result/job-2")

		self.assertEqual(response.status_code, 200)
		payload = response.json()
		self.assertEqual(payload["error"], "Not ready")
		self.assertEqual(payload["progress"], "2/5")

	def test_result_done_payload(self) -> None:
		with patch(
			"api.services.ingest_service.get_job",
			return_value={
				"status": "done",
				"done_chunks": 3,
				"total_chunks": 3,
				"final_output": json.dumps({"abstract": "final"}),
			},
		):
			response = self.client.get("/result/job-3")

		self.assertEqual(response.status_code, 200)
		self.assertEqual(response.json()["abstract"], "final")


if __name__ == "__main__":
	unittest.main()