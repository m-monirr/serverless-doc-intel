import io
import os
import time
import unittest

from fastapi.testclient import TestClient

from api.main import app


class RealPdfIntegrationTests(unittest.TestCase):
	def setUp(self) -> None:
		self.client = TestClient(app)

	def _read_real_pdf(self) -> bytes:
		pdf_path = os.path.join(os.path.dirname(__file__), "sample.pdf")
		with open(pdf_path, "rb") as file_handle:
			return file_handle.read()

	def _assert_result_shape(self, payload: dict) -> None:
		self.assertIsInstance(payload.get("abstract"), str)
		self.assertIsInstance(payload.get("top_key_points"), list)
		self.assertIsInstance(payload.get("documentation"), dict)
		self.assertIsInstance(payload.get("total_chunks"), int)
		self.assertIsInstance(payload.get("failed_chunks"), int)

		docs = payload["documentation"]
		for section in ["introduction", "methods", "findings", "conclusion"]:
			self.assertIn(section, docs)
			self.assertIsInstance(docs[section], str)

	def test_upload_real_pdf_and_poll_until_done(self) -> None:
		pdf_bytes = self._read_real_pdf()
		files = {"file": ("sample.pdf", io.BytesIO(pdf_bytes), "application/pdf")}
		response = self.client.post("/ingest?mode=sequential", files=files)
		self.assertEqual(response.status_code, 200)

		payload = response.json()
		if payload.get("cached"):
			self._assert_result_shape(payload["result"])
			return

		job_id = payload.get("job_id")
		self.assertIsInstance(job_id, str)

		deadline = time.time() + 60
		status_payload = None
		while time.time() < deadline:
			status_response = self.client.get(f"/status/{job_id}")
			self.assertEqual(status_response.status_code, 200)
			status_payload = status_response.json()

			if status_payload["status"] == "done":
				break
			if status_payload["status"] == "error":
				self.fail(f"Pipeline reached error state: {status_payload}")

			time.sleep(0.5)

		self.assertIsNotNone(status_payload)
		self.assertEqual(status_payload["status"], "done")

		result_response = self.client.get(f"/result/{job_id}")
		self.assertEqual(result_response.status_code, 200)
		result_payload = result_response.json()
		self._assert_result_shape(result_payload)


if __name__ == "__main__":
	unittest.main()