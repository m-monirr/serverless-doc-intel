import io
import unittest

from fastapi.testclient import TestClient

from api.main import app


class ApiContractTests(unittest.TestCase):
	def setUp(self) -> None:
		self.client = TestClient(app)

	def test_health_returns_ok(self) -> None:
		response = self.client.get("/health")
		self.assertEqual(response.status_code, 200)
		self.assertEqual(response.json(), {"status": "ok"})

	def test_ingest_rejects_non_pdf(self) -> None:
		files = {"file": ("bad.txt", io.BytesIO(b"hello world"), "text/plain")}
		response = self.client.post("/ingest", files=files)
		self.assertEqual(response.status_code, 422)
		self.assertIn("PDF", response.text)


if __name__ == "__main__":
	unittest.main()