"""PDF extraction and chunking helpers."""

import html
import re

from langchain_text_splitters import RecursiveCharacterTextSplitter

_converter = None
_docling_error: Exception | None = None

try:
	# Preload torch first. On some Windows setups, importing docling (which
	# transitively imports torch) can fail with WinError 1114 unless torch
	# has already initialized its DLLs in-process.
	import torch

	_ = torch.__version__
	from docling.document_converter import DocumentConverter

	print("Loading Docling converter...")
	_converter = DocumentConverter()
	print("Docling ready")
except Exception as exc:
	_docling_error = exc


def _clean_extracted_markdown(text: str) -> str:
	"""Normalize OCR-heavy markdown and remove common low-signal artifacts."""
	decoded = html.unescape(text)
	decoded = re.sub(r"<!--\s*image\s*-->", " ", decoded, flags=re.IGNORECASE)
	decoded = re.sub(r"`{3,}.*?`{3,}", " ", decoded, flags=re.DOTALL)

	clean_lines: list[str] = []
	for raw_line in decoded.splitlines():
		line = " ".join(raw_line.split()).strip()
		if not line:
			continue
		if line.lower() in {"image", "figure", "table"}:
			continue
		# Drop lines that are mostly punctuation/noise from OCR artifacts.
		alnum = sum(ch.isalnum() for ch in line)
		if len(line) >= 20 and alnum / max(1, len(line)) < 0.35:
			continue
		clean_lines.append(line)

	return "\n".join(clean_lines)


def extract_and_chunk(pdf_path: str) -> list[dict[str, str | int]]:
	"""Extract markdown text with Docling and split into chunk dictionaries."""
	if _converter is None:
		message = "Docling is unavailable on this machine. "
		if _docling_error is not None:
			message += f"Reason: {_docling_error}. "
		message += "Using placeholder extraction for local testing."
		return [{"chunk_id": 0, "text": message}]

	result = _converter.convert(pdf_path)
	document = result.document
	full_text = _clean_extracted_markdown(document.export_to_markdown())

	if not full_text.strip():
		raise ValueError(
			"Could not extract text from this PDF. File may be corrupted or unsupported."
		)

	splitter = RecursiveCharacterTextSplitter(
		chunk_size=600,
		chunk_overlap=60,
		separators=["\n## ", "\n### ", "\n\n", "\n", ". ", " "],
	)
	raw_chunks = splitter.split_text(full_text)

	return [{"chunk_id": i, "text": chunk} for i, chunk in enumerate(raw_chunks)]
