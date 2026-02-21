from __future__ import annotations

import json
import re
from typing import Any

import httpx

from app.settings import settings


def _try_parse_json(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        # Best-effort extraction for non-strict outputs.
        m = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", s)
        if m:
            try:
                return json.loads(m.group(1))
            except Exception:
                return s
        return s


async def extract_transactions_from_markdown(markdown: str) -> list[dict[str, Any]]:
    """
    Ask HybrIE VLM to convert docling markdown into a normalized transactions list.
    """
    if not settings.HYBRIE_API_KEY:
        raise RuntimeError("HYBRIE_API_KEY not configured")

    system = (
        "Extract bank statement transactions from the provided document markdown. "
        "Return STRICT JSON only. Schema: {\"transactions\": [{\"date\": \"YYYY-MM-DD|YYYY/MM/DD|DD/MM/YYYY|...\", "
        "\"description\": string, \"amount\": number, \"currency\": string|null, \"balance\": number|null, "
        "\"reference\": string|null}]}. "
        "If the document has multiple accounts, merge all transactions in chronological order. "
        "Do not include any commentary."
    )

    payload = {
        "model": settings.DOCCLI_VISION_MODEL,
        "temperature": 0,
        "max_tokens": 4096,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": markdown},
        ],
    }
    return await _extract_transactions(payload)


async def extract_transactions_from_image_url(image_url: str) -> list[dict[str, Any]]:
    """
    Ask HybrIE VLM to extract transactions directly from image statements.
    """
    if not settings.HYBRIE_API_KEY:
        raise RuntimeError("HYBRIE_API_KEY not configured")

    system = (
        "Extract bank statement transactions from the provided image. "
        "Return STRICT JSON only. Schema: {\"transactions\": [{\"date\": \"YYYY-MM-DD|YYYY/MM/DD|DD/MM/YYYY|...\", "
        "\"description\": string, \"amount\": number, \"currency\": string|null, \"balance\": number|null, "
        "\"reference\": string|null}]}. "
        "Do not include any commentary."
    )

    payload = {
        "model": settings.DOCCLI_VISION_MODEL,
        "temperature": 0,
        "max_tokens": 4096,
        "messages": [
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Extract bank statement transactions from this image."},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            },
        ],
    }
    return await _extract_transactions(payload)


async def _extract_transactions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(
            f"{settings.HYBRIE_API_URL.rstrip('/')}/v1/chat/completions",
            headers={"Authorization": f"Bearer {settings.HYBRIE_API_KEY}"},
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()

    content = (
        (((data.get("choices") or [{}])[0].get("message") or {}).get("content"))
        if isinstance(data, dict)
        else None
    )
    if isinstance(content, list):
        text_parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text_parts.append(str(item.get("text") or ""))
        content = "\n".join(text_parts).strip()
    if not isinstance(content, str):
        raise RuntimeError("Unexpected HybrIE response format")

    parsed = _try_parse_json(content)
    if isinstance(parsed, dict) and isinstance(parsed.get("transactions"), list):
        return list(parsed["transactions"])
    if isinstance(parsed, list):
        return list(parsed)
    raise RuntimeError("Model did not return a transactions list")
