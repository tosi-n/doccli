# doccli

Internal tool-service (FastAPI) for bank statement PDF processing used by `stimulir-console`.

Scope (v1):
- Bank statement PDF -> transactions JSON + CSV
- Conversion pipeline: Docling extraction + HybrIE VLM (`Qwen/Qwen2.5-VL-72B-Instruct`)
- Durable job execution via Choreo worker

This service is self-contained and focused on internal document extraction workflows.

## Run (local)

API:
```bash
docker build -t doccli:dev -f service/Dockerfile .
docker run --rm -p 8000:8000 --env-file .env doccli:dev
```

Worker:
```bash
docker build -t doccli-worker:dev -f worker/Dockerfile .
docker run --rm --env-file .env doccli-worker:dev
```

## Internal API (called by stimulir backend)
- `POST /internal/bank-statement/jobs`
- `GET /internal/bank-statement/jobs/{job_id}`
- `GET /internal/bank-statement/jobs/{job_id}/transactions`
- `GET /internal/bank-statement/jobs/{job_id}/csv`

Internal endpoints accept `X-Internal-API-Key: $DOCCLI_INTERNAL_API_KEY` when configured.
If `DOCCLI_INTERNAL_API_KEY` is empty, auth is disabled for private-network deployments.
