"""Modal ASGI endpoint that provides chat and embeddings APIs.

This is a pragmatic OpenAI-style interface used by the local API:
- POST /v1/chat/completions
- POST /v1/embeddings
"""

from functools import lru_cache
from typing import Any

import modal
from pydantic import BaseModel, Field

app = modal.App("pdf-vllm-server-v2")

image = (
	modal.Image.debian_slim(python_version="3.12")
	.pip_install(
		"fastapi==0.115.11",
		"pydantic==2.11.5",
		"sentence-transformers==3.0.1",
		"transformers==4.44.2",
		"torch==2.4.1",
	)
)


class ChatMessage(BaseModel):
	role: str
	content: str


class ChatRequest(BaseModel):
	model: str | None = None
	messages: list[ChatMessage]
	temperature: float = 0.2
	max_tokens: int = 512


class EmbeddingsRequest(BaseModel):
	model: str | None = None
	input: list[str] = Field(default_factory=list)


@app.function(image=image, timeout=1800, scaledown_window=300)
@modal.asgi_app()
def asgi_app_v2():
	from fastapi import Body, FastAPI, HTTPException
	from sentence_transformers import SentenceTransformer
	from transformers import AutoModelForCausalLM, AutoTokenizer
	import torch

	web = FastAPI(title="PDF LLM Service", version="1.0.0")

	CHAT_MODEL_NAME = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"
	EMBED_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

	@lru_cache(maxsize=1)
	def get_chat_artifacts():
		tokenizer = AutoTokenizer.from_pretrained(CHAT_MODEL_NAME)
		model = AutoModelForCausalLM.from_pretrained(CHAT_MODEL_NAME)
		model.eval()
		return tokenizer, model

	@lru_cache(maxsize=1)
	def get_embedder():
		return SentenceTransformer(EMBED_MODEL_NAME)

	@web.get("/")
	def root() -> dict[str, Any]:
		return {
			"service": "pdf-vllm-server",
			"status": "ok",
			"release": "2026-04-17-routes-debug",
			"endpoints": ["/v1/chat/completions", "/v1/embeddings"],
		}

	@web.get("/debug/routes")
	def debug_routes() -> list[dict[str, Any]]:
		rows: list[dict[str, Any]] = []
		for route in web.routes:
			rows.append(
				{
					"path": getattr(route, "path", ""),
					"name": getattr(route, "name", ""),
					"methods": sorted(list(getattr(route, "methods", []) or [])),
				}
			)
		return rows

	@web.post("/v1/chat/completions")
	def chat_completions(request: ChatRequest = Body(...)) -> dict[str, Any]:
		if not request.messages:
			raise HTTPException(status_code=400, detail="messages cannot be empty")

		try:
			tokenizer, model = get_chat_artifacts()
			prompt = "\n".join(f"{m.role}: {m.content}" for m in request.messages)
			inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=2048)
			with torch.no_grad():
				outputs = model.generate(
					**inputs,
					max_new_tokens=max(32, min(1024, int(request.max_tokens))),
					do_sample=request.temperature > 0,
					temperature=max(0.0, float(request.temperature)),
					pad_token_id=tokenizer.eos_token_id,
				)
			full_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
			answer = full_text[len(prompt) :].strip() if full_text.startswith(prompt) else full_text.strip()
			if not answer:
				answer = "No response generated."
		except Exception as exc:
			raise HTTPException(status_code=500, detail=f"chat generation failed: {exc}")

		return {
			"id": "chatcmpl-modal",
			"object": "chat.completion",
			"choices": [
				{
					"index": 0,
					"message": {"role": "assistant", "content": answer},
					"finish_reason": "stop",
				}
			],
		}

	@web.post("/v1/embeddings")
	def embeddings(request: EmbeddingsRequest = Body(...)) -> dict[str, Any]:
		if not request.input:
			raise HTTPException(status_code=400, detail="input cannot be empty")

		try:
			embedder = get_embedder()
			vectors = embedder.encode(request.input, convert_to_numpy=True)
		except Exception as exc:
			raise HTTPException(status_code=500, detail=f"embedding failed: {exc}")

		data = [
			{"object": "embedding", "index": i, "embedding": vectors[i].tolist()}
			for i in range(len(request.input))
		]
		return {"object": "list", "data": data, "model": request.model or EMBED_MODEL_NAME}

	return web
