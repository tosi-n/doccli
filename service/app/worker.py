from __future__ import annotations

import asyncio
import csv
import io
import json
import time
import urllib.parse
from typing import Any

import httpx
from sqlalchemy import select, update

from app.choreo_runtime import choreo
from app.db import BankStatementJob, init_db, session_scope
from app.docling_pipeline import pdf_to_markdown
from app.hybrie_client import (
    extract_transactions_from_image_url,
    extract_transactions_from_markdown,
)
from app.settings import settings


async def _backend_upload_bytes(
    *,
    business_profile_id: str,
    provider: str,
    filename: str,
    content_type: str,
    data: bytes,
    metadata: dict[str, Any] | None = None,
) -> str:
    files = {"file": (filename, data, content_type)}
    form = {
        "business_profile_id": business_profile_id,
        "provider": provider,
        "filename": filename,
    }
    if metadata:
        form["metadata_json"] = json.dumps(metadata)
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(
            settings.BACKEND_INTERNAL_UPLOAD_URL,
            headers={"X-Internal-API-Key": settings.DOCCLI_INTERNAL_API_KEY},
            data=form,
            files=files,
        )
        resp.raise_for_status()
        out = resp.json()
        url = out.get("url")
        if not url:
            raise RuntimeError("Backend upload returned no url")
        return str(url)


def _transactions_to_csv_bytes(transactions: list[dict[str, Any]]) -> bytes:
    buf = io.StringIO()
    fieldnames = ["date", "description", "amount", "currency", "balance", "reference"]
    w = csv.DictWriter(buf, fieldnames=fieldnames)
    w.writeheader()
    for t in transactions:
        w.writerow({k: (t.get(k) if k in t else None) for k in fieldnames})
    return buf.getvalue().encode("utf-8")


def _is_image_statement(input_url: str, content_type: str | None) -> bool:
    ctype = (content_type or "").lower()
    if ctype.startswith("image/"):
        return True
    path = urllib.parse.urlparse(input_url).path.lower()
    return path.endswith((".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif"))


async def _set_failed(job_id: str, error: str) -> None:
    async for db in session_scope():
        await db.execute(
            update(BankStatementJob)
            .where(BankStatementJob.id == job_id)
            .values(
                status="failed",
                progress=1.0,
                error=error[:2000],
            )
        )
        await db.commit()


@choreo.function("doccli-bank-statement-process", trigger="doccli.bank_statement.process", retries=3, timeout=1800)
async def process_job(ctx, step):
    job_id = str(ctx.data.get("job_id") or "")
    if not job_id:
        return {"outcome": "skipped", "reason": "missing_job_id"}

    async for db in session_scope():
        res = await db.execute(select(BankStatementJob).where(BankStatementJob.id == job_id))
        job = res.scalars().first()
        if not job:
            return {"outcome": "skipped", "reason": "job_not_found"}

        await db.execute(
            update(BankStatementJob)
            .where(BankStatementJob.id == job_id)
            .values(status="processing", progress=0.05, error=None)
        )
        await db.commit()

    try:
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            src_resp = await client.get(job.input_file_url)
            src_resp.raise_for_status()
            file_bytes = src_resp.content
            source_content_type = src_resp.headers.get("content-type")

        await step.run("mark-progress-20", lambda: _set_progress(job_id, 0.2))
        if _is_image_statement(job.input_file_url, source_content_type):
            txs = await step.run(
                "hybrie-extract-image",
                lambda: extract_transactions_from_image_url(job.input_file_url),
            )
            await step.run("mark-progress-60", lambda: _set_progress(job_id, 0.6))
        else:
            markdown = await step.run("docling-to-markdown", lambda: pdf_to_markdown(file_bytes))
            await step.run("mark-progress-60", lambda: _set_progress(job_id, 0.6))
            txs = await step.run("hybrie-extract-markdown", lambda: extract_transactions_from_markdown(markdown))

        await step.run("mark-progress-80", lambda: _set_progress(job_id, 0.8))

        json_bytes = json.dumps({"transactions": txs}, indent=2).encode("utf-8")
        csv_bytes = _transactions_to_csv_bytes(txs)

        out_json_url = await step.run(
            "upload-json",
            lambda: _backend_upload_bytes(
                business_profile_id=job.business_profile_id,
                provider="doccli",
                filename=f"bank_statement_{job_id}.json",
                content_type="application/json",
                data=json_bytes,
                metadata={"job_id": job_id},
            ),
        )
        out_csv_url = await step.run(
            "upload-csv",
            lambda: _backend_upload_bytes(
                business_profile_id=job.business_profile_id,
                provider="doccli",
                filename=f"bank_statement_{job_id}.csv",
                content_type="text/csv",
                data=csv_bytes,
                metadata={"job_id": job_id},
            ),
        )

        async for db in session_scope():
            await db.execute(
                update(BankStatementJob)
                .where(BankStatementJob.id == job_id)
                .values(
                    status="completed",
                    progress=1.0,
                    transactions=txs,
                    output_csv_url=out_csv_url,
                    output_json_url=out_json_url,
                    error=None,
                )
            )
            await db.commit()

        return {"outcome": "ok", "job_id": job_id, "count": len(txs)}
    except Exception as e:
        await step.run("mark-failed", lambda: _set_failed(job_id, str(e)))
        return {"outcome": "failed", "job_id": job_id, "reason": str(e)}


async def _set_progress(job_id: str, progress: float) -> None:
    async for db in session_scope():
        await db.execute(
            update(BankStatementJob)
            .where(BankStatementJob.id == job_id)
            .values(progress=progress)
        )
        await db.commit()


async def main() -> None:
    await init_db()
    await choreo.start_worker()


if __name__ == "__main__":
    asyncio.run(main())
