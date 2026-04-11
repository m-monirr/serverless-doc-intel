# Modal Credentials Setup

Use this once to prepare all credentials and secrets.

## 1) Install and login

```powershell
pip install modal
modal token new
```

After `modal token new`, copy the values into `.env`:

- `MODAL_TOKEN_ID`
- `MODAL_TOKEN_SECRET`

## 2) Redis URL

For local dev, keep:

- `REDIS_URL=redis://localhost:6379`

For production, create Upstash and copy its Redis URL into `REDIS_URL`.

## 3) Deploy GPU endpoint first

```powershell
modal deploy modal/vllm_server.py
```

Copy the deployed URL (for example `https://xxx.modal.run`) into:

- `.env` as `MODAL_VLLM_URL`

## 4) Create Modal secrets

Run these after `.env` is filled:

```powershell
modal secret create pdf-pipeline-secrets \
  REDIS_URL=$env:REDIS_URL \
  MODAL_TOKEN_ID=$env:MODAL_TOKEN_ID \
  MODAL_TOKEN_SECRET=$env:MODAL_TOKEN_SECRET \
  MODAL_VLLM_URL=$env:MODAL_VLLM_URL
```

## 5) Deploy worker

```powershell
modal deploy modal/worker.py
```

The worker app/function names used by the API are:

- `MODAL_WORKER_APP=pdf-pipeline-worker`
- `MODAL_WORKER_FUNCTION=process_chunk_remote`

To enable remote execution in the API process, set:

- `USE_MODAL_REMOTE=1`
- `MODAL_LOCAL_FALLBACK=1` (recommended for resilience)

## 6) Quick checklist

- `MODAL_TOKEN_ID` is set
- `MODAL_TOKEN_SECRET` is set
- `MODAL_VLLM_URL` is set
- `REDIS_URL` is set
- `ALLOW_FAKE_REDIS=1` for local fallback testing
- `USE_MODAL_REMOTE=1` when testing cloud worker path

