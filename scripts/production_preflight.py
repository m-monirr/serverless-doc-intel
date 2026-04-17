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


def _check_modal_credentials() -> CheckResult:
	token_id = os.getenv("MODAL_TOKEN_ID", "").strip()
	token_secret = os.getenv("MODAL_TOKEN_SECRET", "").strip()
	use_modal_remote = os.getenv("USE_MODAL_REMOTE", "0").strip().lower() in {"1", "true", "yes", "on"}

	placeholder_tokens = {"", "your_modal_token_id", "your_modal_token_secret"}
	id_is_placeholder = token_id.lower() in placeholder_tokens
	secret_is_placeholder = token_secret.lower() in placeholder_tokens

	# Catch committed credential-like values and force operator action.
	id_looks_real = token_id.startswith("ak-")
	secret_looks_real = token_secret.startswith("as-")

	if id_looks_real or secret_looks_real:
		return CheckResult(
			"modal_credentials",
			"fail",
			"Modal credentials look like real tokens in .env. Move to secure secrets store and rotate tokens.",
		)

	if use_modal_remote and (id_is_placeholder or secret_is_placeholder):
		return CheckResult(
			"modal_credentials",
			"fail",
			"USE_MODAL_REMOTE=1 requires MODAL_TOKEN_ID and MODAL_TOKEN_SECRET.",
		)

	if id_is_placeholder and secret_is_placeholder:
		return CheckResult(
			"modal_credentials",
			"warn",
			"Modal credentials are not set. This is fine for local-only mode.",
		)

	return CheckResult("modal_credentials", "pass", "Modal credentials are configured.")


def _check_redis() -> CheckResult:
	redis_url = os.getenv("REDIS_URL", "").strip()
	allow_fake_redis = os.getenv("ALLOW_FAKE_REDIS", "1").strip() in {"1", "true", "yes", "on"}
	if not redis_url:
		if allow_fake_redis:
			return CheckResult(
				"redis",
				"warn",
				"REDIS_URL is empty but ALLOW_FAKE_REDIS=1, local fallback will be used.",
			)
		return CheckResult("redis", "fail", "REDIS_URL is empty.")

	try:
		client = redis.from_url(redis_url, decode_responses=True)
		client.ping()
		return CheckResult("redis", "pass", "Redis ping succeeded.")
	except Exception as exc:
		if allow_fake_redis:
			return CheckResult(
				"redis",
				"warn",
				f"Redis ping failed but ALLOW_FAKE_REDIS=1, fallback is active: {exc}",
			)
		return CheckResult("redis", "fail", f"Redis ping failed: {exc}")


def _check_vllm_chat() -> CheckResult:
	mode = llm_mode_status()
	if not mode.get("use_llm_analysis", False):
		return CheckResult(
			"vllm_chat",
			"warn",
			"USE_LLM_ANALYSIS is disabled, skipping live chat endpoint probe.",
		)

	vllm_url = os.getenv("MODAL_VLLM_CHAT_URL", "").strip() or os.getenv("MODAL_VLLM_URL", "").strip()
	if any(token in vllm_url.lower() for token in ["placeholder", "your_endpoint", "example"]):
		return CheckResult(
			"vllm_chat",
			"fail",
			"vLLM URL appears to be a placeholder value; set a real deployed endpoint URL.",
		)

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

	embedding_url = os.getenv("EMBEDDING_API_URL", "").strip()
	if any(token in embedding_url.lower() for token in ["placeholder", "your_endpoint", "example"]):
		return CheckResult(
			"embeddings",
			"fail",
			"Embedding URL appears to be a placeholder value; set a real /v1/embeddings endpoint.",
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
		_check_modal_credentials(),
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
