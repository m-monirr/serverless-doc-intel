"""Lightweight embedding-style retrieval for chunk selection."""

import hashlib
from typing import Any

import numpy as np

from Modal.llm_client import USE_REAL_EMBEDDINGS, get_text_embeddings


def _tokenize(text: str) -> list[str]:
	return [tok for tok in text.lower().split() if tok]


def _token_vector(token: str, dim: int) -> np.ndarray:
	# Stable token projection using md5 seed for deterministic behavior.
	seed = int(hashlib.md5(token.encode("utf-8")).hexdigest()[:8], 16)
	rng = np.random.default_rng(seed)
	vec = rng.standard_normal(dim)
	norm = np.linalg.norm(vec)
	if norm == 0:
		return vec
	return vec / norm


def embed_text(text: str, dim: int = 128) -> np.ndarray:
	tokens = _tokenize(text)
	if not tokens:
		return np.zeros(dim)

	vec = np.zeros(dim)
	for token in tokens:
		vec += _token_vector(token, dim)

	norm = np.linalg.norm(vec)
	if norm == 0:
		return vec
	return vec / norm


def select_representative_chunk_ids(
	chunks: list[dict[str, Any]],
	top_k: int = 8,
	dim: int = 128,
) -> list[int]:
	"""Select representative chunks by similarity to document centroid embedding."""
	if not chunks:
		return []

	embeddings: list[np.ndarray] = []
	chunk_ids: list[int] = []
	chunk_texts: list[str] = []
	for chunk in chunks:
		chunk_ids.append(int(chunk.get("chunk_id", 0)))
		chunk_texts.append(str(chunk.get("text", "")))

	if USE_REAL_EMBEDDINGS:
		try:
			remote_vecs = get_text_embeddings(chunk_texts)
			for vec in remote_vecs:
				np_vec = np.array(vec, dtype=float)
				norm = np.linalg.norm(np_vec)
				if norm > 0:
					np_vec = np_vec / norm
				embeddings.append(np_vec)
		except Exception:
			# Safe fallback keeps pipeline available if embedding endpoint is down.
			embeddings = [embed_text(text, dim=dim) for text in chunk_texts]
	else:
		embeddings = [embed_text(text, dim=dim) for text in chunk_texts]

	mat = np.vstack(embeddings)
	centroid = mat.mean(axis=0)
	centroid_norm = np.linalg.norm(centroid)
	if centroid_norm > 0:
		centroid = centroid / centroid_norm

	scores = mat @ centroid
	# Mild length bias to avoid tiny chunks dominating similarity.
	length_bonus = np.array([
		min(len(str(chunk.get("text", ""))) / 2000.0, 0.1)
		for chunk in chunks
	])
	scores = scores + length_bonus

	k = max(1, min(top_k, len(chunks)))
	top_indices = np.argsort(-scores)[:k]
	selected = sorted({chunk_ids[i] for i in top_indices})
	return selected
