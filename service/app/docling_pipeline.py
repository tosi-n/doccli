from __future__ import annotations

import tempfile
from typing import Any


async def pdf_to_markdown(pdf_bytes: bytes) -> str:
    """
    Convert PDF bytes to markdown using Docling.

    Docling usage is based on official docs:
    https://docling-project.github.io/docling/
    """
    from docling.document_converter import DocumentConverter  # type: ignore

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as f:
        f.write(pdf_bytes)
        f.flush()
        converter = DocumentConverter()
        result = converter.convert(f.name)
        return result.document.export_to_markdown()

