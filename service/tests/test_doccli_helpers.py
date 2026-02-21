from __future__ import annotations

import os

os.environ.setdefault("DOCCLI_INTERNAL_API_KEY", "test-doccli-key")

from app.hybrie_client import _try_parse_json
from app.worker import _is_image_statement


def test_is_image_statement_from_content_type() -> None:
    assert _is_image_statement("https://example.com/statement.bin", "image/png")
    assert not _is_image_statement("https://example.com/statement.bin", "application/pdf")


def test_is_image_statement_from_url_extension() -> None:
    assert _is_image_statement("https://example.com/statement.jpeg", None)
    assert _is_image_statement("https://example.com/a/b/scan.heic", "")
    assert not _is_image_statement("https://example.com/statement.pdf", None)


def test_try_parse_json_returns_structured_data_when_possible() -> None:
    parsed = _try_parse_json('prefix {"transactions":[{"amount":12.5}]} suffix')
    assert isinstance(parsed, dict)
    assert parsed["transactions"][0]["amount"] == 12.5


def test_try_parse_json_falls_back_to_raw_text() -> None:
    text = "not-json"
    assert _try_parse_json(text) == text
