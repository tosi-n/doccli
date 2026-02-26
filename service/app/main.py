from __future__ import annotations

import os
from typing import Any
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, update

from app.choreo_runtime import choreo
from app.db import BankStatementJob, init_db, session_scope
from app.internal_auth import require_internal_api_key


app = FastAPI(title="doccli", version="0.1.0")


@app.on_event("startup")
async def _startup() -> None:
    os.makedirs("/data", exist_ok=True)
    await init_db()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


class CreateJobIn(BaseModel):
    business_profile_id: UUID
    user_id: UUID
    input_file_url: str
    input_document_id: str | None = None
    month_key: str | None = None
    currency: str | None = None


@app.post("/internal/bank-statement/jobs", dependencies=[Depends(require_internal_api_key)])
async def create_job(body: CreateJobIn) -> dict[str, Any]:
    async for db in session_scope():
        job = BankStatementJob(
            business_profile_id=str(body.business_profile_id),
            user_id=str(body.user_id),
            input_file_url=body.input_file_url,
            input_document_id=body.input_document_id,
            month_key=body.month_key,
            currency=body.currency,
            status="queued",
            progress=0.0,
        )
        db.add(job)
        await db.commit()
        await db.refresh(job)

    event = await choreo.send(
        "doccli.bank_statement.process",
        {
            "job_id": job.id,
            "business_profile_id": str(body.business_profile_id),
            "user_id": str(body.user_id),
        },
        idempotency_key=f"doccli:bank_statement:{job.id}",
        user_id=str(body.user_id),
    )

    return {"job_id": job.id, "choreo_event_id": event.get("event_id"), "choreo_run_ids": event.get("run_ids")}


@app.get("/internal/bank-statement/jobs/{job_id}", dependencies=[Depends(require_internal_api_key)])
async def get_job(job_id: str) -> dict[str, Any]:
    async for db in session_scope():
        res = await db.execute(select(BankStatementJob).where(BankStatementJob.id == job_id))
        job = res.scalars().first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {
            "job_id": job.id,
            "status": job.status,
            "progress": job.progress,
            "output_csv_url": job.output_csv_url,
            "output_json_url": job.output_json_url,
            "output_csv_document_id": job.output_csv_document_id,
            "output_json_document_id": job.output_json_document_id,
            "error": job.error,
        }


@app.get("/internal/bank-statement/jobs/{job_id}/transactions", dependencies=[Depends(require_internal_api_key)])
async def get_transactions(job_id: str) -> list[dict[str, Any]]:
    async for db in session_scope():
        res = await db.execute(select(BankStatementJob).where(BankStatementJob.id == job_id))
        job = res.scalars().first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return list(job.transactions or [])


@app.get("/internal/bank-statement/jobs/{job_id}/csv", dependencies=[Depends(require_internal_api_key)])
async def get_csv(job_id: str) -> dict[str, Any]:
    async for db in session_scope():
        res = await db.execute(select(BankStatementJob).where(BankStatementJob.id == job_id))
        job = res.scalars().first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {
            "output_csv_url": job.output_csv_url,
            "output_csv_document_id": job.output_csv_document_id,
        }
