from typing import Any

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, Request, UploadFile
from fastapi.responses import Response, StreamingResponse

load_dotenv()

from api.services.ingest_service import (
	get_quota,
	get_result,
	get_result_markdown,
	get_runtime_observability,
	get_status,
	ingest_pdf,
	stream_events,
)

app = FastAPI(title="Async PDF Pipeline", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
	return {"status": "ok"}


@app.post("/ingest")
async def ingest(
	request: Request,
	file: UploadFile,
	background_tasks: BackgroundTasks,
	mode: str = "parallel",
):
	return await ingest_pdf(request, file, background_tasks, mode=mode)


@app.get("/status/{job_id}")
def status(job_id: str) -> dict[str, Any]:
	return get_status(job_id)


@app.get("/result/{job_id}")
def result(job_id: str) -> dict[str, Any]:
	return get_result(job_id)


@app.get("/result/{job_id}/markdown")
def result_markdown(job_id: str) -> Response:
	markdown = get_result_markdown(job_id)
	headers = {"Content-Disposition": f'attachment; filename="report-{job_id}.md"'}
	return Response(content=markdown, media_type="text/markdown", headers=headers)


@app.get("/quota")
def quota(request: Request) -> dict[str, int]:
	return get_quota(request)


@app.get("/observability")
def observability() -> dict[str, Any]:
	return get_runtime_observability()


@app.get("/stream/{job_id}")
async def stream(job_id: str) -> StreamingResponse:
	return StreamingResponse(stream_events(job_id), media_type="text/event-stream")
