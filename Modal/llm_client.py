"""Shared vLLM client helpers for worker and aggregation paths."""

import json
import os
from typing import Any

import requests


def _is_enabled(value: str | None, default: bool = False) -> bool:
	if value is None:
		return default
	return value.strip().lower() in {"1", "true", "yes", "on"}


USE_LLM_ANALYSIS = _is_enabled(os.getenv("USE_LLM_ANALYSIS"), default=False)
ALLOW_LLM_FALLBACK = _is_enabled(os.getenv("ALLOW_LLM_FALLBACK"), default=True)
MODAL_VLLM_URL = os.getenv("MODAL_VLLM_URL", "").strip()
MODAL_VLLM_MODEL = os.getenv("MODAL_VLLM_MODEL", "Qwen2.5-7B-Instruct")
MODAL_VLLM_API_KEY = os.getenv("MODAL_VLLM_API_KEY", "").strip()
VLLM_TIMEOUT_SECONDS = int(os.getenv("VLLM_TIMEOUT_SECONDS", "90"))


def llm_mode_status() -> dict[str, Any]:
	"""Expose current LLM execution mode and endpoint config."""
	return {
		"use_llm_analysis": USE_LLM_ANALYSIS,
		"allow_llm_fallback": ALLOW_LLM_FALLBACK,
		"vllm_url_configured": bool(MODAL_VLLM_URL),
		"vllm_model": MODAL_VLLM_MODEL,
	}


def _extract_text_from_response(payload: Any) -> str:
	if isinstance(payload, str):
		return payload
	if not isinstance(payload, dict):
		return json.dumps(payload)

	choices = payload.get("choices")
	if isinstance(choices, list) and choices:
		first = choices[0]
		if isinstance(first, dict):
			message = first.get("message")
			if isinstance(message, dict) and isinstance(message.get("content"), str):
				return message["content"]
			if isinstance(first.get("text"), str):
				return first["text"]

	for key in ["output", "text", "generated_text", "response", "result"]:
		val = payload.get(key)
		if isinstance(val, str):
			return val

	return json.dumps(payload)


def call_vllm_prompt(prompt: str, *, max_tokens: int = 700, temperature: float = 0.2) -> str:
	"""Call configured vLLM endpoint and return text output."""
	if not MODAL_VLLM_URL:
		raise RuntimeError("MODAL_VLLM_URL is not configured")

	headers = {"Content-Type": "application/json"}
	if MODAL_VLLM_API_KEY:
		headers["Authorization"] = f"Bearer {MODAL_VLLM_API_KEY}"

	if "/v1/chat/completions" in MODAL_VLLM_URL:
		body = {
			"model": MODAL_VLLM_MODEL,
			"messages": [
				{
					"role": "system",
					"content": "You are a precise PDF analyst. Return concise, structured results.",
				},
				{"role": "user", "content": prompt},
			],
			"temperature": temperature,
			"max_tokens": max_tokens,
		}
	else:
		# Generic endpoint fallback payload.
		body = {
			"prompt": prompt,
			"temperature": temperature,
			"max_tokens": max_tokens,
			"model": MODAL_VLLM_MODEL,
		}

	resp = requests.post(
		MODAL_VLLM_URL,
		json=body,
		headers=headers,
		timeout=VLLM_TIMEOUT_SECONDS,
	)
	resp.raise_for_status()

	try:
		payload = resp.json()
	except json.JSONDecodeError:
		return resp.text

	return _extract_text_from_response(payload)


def parse_json_object(text: str) -> dict[str, Any]:
	"""Extract and parse first JSON object from model text."""
	candidate = text.strip()
	if candidate.startswith("```"):
		candidate = candidate.strip("`")
		if candidate.lower().startswith("json"):
			candidate = candidate[4:].strip()

	try:
		obj = json.loads(candidate)
		if isinstance(obj, dict):
			return obj
	except json.JSONDecodeError:
		pass

	start = candidate.find("{")
	end = candidate.rfind("}")
	if start == -1 or end == -1 or end <= start:
		raise ValueError("No JSON object found in model output")

	obj = json.loads(candidate[start : end + 1])
	if not isinstance(obj, dict):
		raise ValueError("Parsed model output is not a JSON object")
	return obj
