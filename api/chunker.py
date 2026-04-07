"""PDF extraction and chunking helpers."""

from docling.document_converter import DocumentConverter
from langchain_text_splitters import RecursiveCharacterTextSplitter

print("Loading Docling converter...")
_converter = DocumentConverter()
print("Docling ready")


def extract_and_chunk(pdf_path: str) -> list[dict[str, str | int]]:
	"""Extract markdown text with Docling and split into chunk dictionaries."""
	result = _converter.convert(pdf_path)
	document = result.document
	full_text = document.export_to_markdown()

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
