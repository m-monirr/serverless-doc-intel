"""Production preflight checks for PDF review service."""

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import redis
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
	sys.path.insert(0, str(PROJECT_ROOT))

from Modal.llm_client import (
	USE_REAL_EMBEDDINGS,
	call_vllm_prompt,
	get_text_embeddings,
	llm_mode_status,
)


@dataclass
class CheckResult:
	name: str
	status: str
	message: str

	def as_dict(self) -> dict[str, str]:
		return {"name": self.name, "status": self.status, "message": self.message}


def _check_env_mode() -> CheckResult:
	mode = llm_mode_status()
	if not mode.get("use_llm_analysis", False):
		return CheckResult(
			"llm_mode",
			"warn",
			"USE_LLM_ANALYSIS is disabled. System will use fallback analysis.",
		)
	if not mode.get("vllm_url_configured", False):
		return CheckResult(
			"llm_mode",
			"fail",
			"USE_LLM_ANALYSIS is enabled but vLLM URL is not configured.",
		)
	return CheckResult("llm_mode", "pass", "LLM mode is enabled with endpoint configured.")


def _check_redis() -> CheckResult:
	redis_url = os.getenv("REDIS_URL", "").strip()
	if not redis_url:
		return CheckResult("redis", "fail", "REDIS_URL is empty.")

	try:
		client = redis.from_url(redis_url, decode_responses=True)
		client.ping()
		return CheckResult("redis", "pass", "Redis ping succeeded.")
	except Exception as exc:
		return CheckResult("redis", "fail", f"Redis ping failed: {exc}")


def _check_vllm_chat() -> CheckResult:
	try:
		_ = call_vllm_prompt("Reply with exactly: ok", max_tokens=8, temperature=0)
		return CheckResult("vllm_chat", "pass", "vLLM chat request succeeded.")
	except Exception as exc:
		return CheckResult("vllm_chat", "fail", f"vLLM chat request failed: {exc}")


def _check_embeddings() -> CheckResult:
	if not USE_REAL_EMBEDDINGS:
		return CheckResult(
			"embeddings",
			"warn",
			"USE_REAL_EMBEDDINGS is disabled. Retrieval uses local deterministic embeddings.",
		)

	try:
		vectors = get_text_embeddings(["health check embedding"])
		if not vectors or not vectors[0]:
			return CheckResult("embeddings", "fail", "Embedding endpoint returned empty vectors.")
		return CheckResult("embeddings", "pass", "Embedding endpoint request succeeded.")
	except Exception as exc:
		return CheckResult("embeddings", "fail", f"Embedding request failed: {exc}")


def main() -> int:
	load_dotenv()

	checks = [
		_check_env_mode(),
		_check_redis(),
		_check_vllm_chat(),
		_check_embeddings(),
	]

	summary: dict[str, Any] = {
		"checks": [c.as_dict() for c in checks],
		"pass_count": sum(1 for c in checks if c.status == "pass"),
		"warn_count": sum(1 for c in checks if c.status == "warn"),
		"fail_count": sum(1 for c in checks if c.status == "fail"),
	}

	print(json.dumps(summary, indent=2))
	return 1 if summary["fail_count"] > 0 else 0


if __name__ == "__main__":
	raise SystemExit(main())
