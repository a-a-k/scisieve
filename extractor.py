from __future__ import annotations

import asyncio
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

import httpx

from api_clients import PoliteRateLimiter

try:
    import fitz  # type: ignore
except Exception:  # pragma: no cover
    fitz = None


KEYWORD_GROUPS: dict[str, list[str]] = {
    "M": ["system model", "architecture", "digital twin"],
    "D": ["fault model", "workload", "injection", "perturbation"],
    "E": ["simulation", "analytical model", "verification engine"],
    "Metrics": ["TTD", "TTR", "MTTR", "blast radius", "VaR"],
}

_HEADING_RE = re.compile(r"^(?:\d+(?:\.\d+){0,3}\s+)?[A-Z][A-Za-z0-9 ,:;()/\-\[\]]{2,90}$")


def best_pdf_url_from_work(work: Mapping[str, Any]) -> str | None:
    best_oa_location = work.get("best_oa_location")
    if isinstance(best_oa_location, dict):
        pdf_url = best_oa_location.get("pdf_url")
        if isinstance(pdf_url, str) and pdf_url.strip():
            return pdf_url.strip()

    primary_location = work.get("primary_location")
    if isinstance(primary_location, dict):
        pdf_url = primary_location.get("pdf_url")
        if isinstance(pdf_url, str) and pdf_url.strip():
            return pdf_url.strip()

    locations = work.get("locations")
    if isinstance(locations, list):
        for loc in locations:
            if not isinstance(loc, dict):
                continue
            pdf_url = loc.get("pdf_url")
            if isinstance(pdf_url, str) and pdf_url.strip():
                return pdf_url.strip()
    return None


def _split_into_sections(full_text: str) -> list[tuple[str, str]]:
    lines = [line.strip() for line in full_text.splitlines()]
    sections: list[tuple[str, str]] = []
    current_heading = "FULL_TEXT"
    current_lines: list[str] = []

    for line in lines:
        if not line:
            continue
        looks_like_heading = (
            bool(_HEADING_RE.match(line))
            and len(line.split()) <= 12
            and not line.endswith(".")
            and len(line) <= 100
        )
        if looks_like_heading:
            if current_lines:
                sections.append((current_heading, "\n".join(current_lines)))
                current_lines = []
            current_heading = line
        else:
            current_lines.append(line)

    if current_lines:
        sections.append((current_heading, "\n".join(current_lines)))
    return sections or [("FULL_TEXT", full_text)]


def _matched_keywords(section_text: str, keywords: Sequence[str]) -> list[str]:
    lowered = section_text.lower()
    hits: list[str] = []
    for keyword in keywords:
        token = keyword.lower()
        if " " in token or "-" in token:
            if token in lowered:
                hits.append(keyword)
        else:
            if re.search(rf"\b{re.escape(token)}\b", lowered):
                hits.append(keyword)
    return hits


def extract_framework_sections_from_text(
    full_text: str, keyword_groups: Mapping[str, Sequence[str]] = KEYWORD_GROUPS
) -> dict[str, list[dict[str, Any]]]:
    sections = _split_into_sections(full_text)
    output: dict[str, list[dict[str, Any]]] = {group: [] for group in keyword_groups}
    for heading, section_text in sections:
        for group_name, keywords in keyword_groups.items():
            hits = _matched_keywords(section_text, keywords)
            if hits:
                output[group_name].append(
                    {
                        "section_heading": heading,
                        "matched_keywords": hits,
                        "text": section_text[:8000],
                    }
                )
    return output


def extract_framework_sections_from_pdf(pdf_path: Path) -> dict[str, list[dict[str, Any]]]:
    if fitz is None:
        raise RuntimeError("PyMuPDF is required for PDF extraction. Install `pymupdf`.")
    doc = fitz.open(str(pdf_path))
    full_text = []
    for page_index in range(doc.page_count):
        full_text.append(doc.load_page(page_index).get_text("text"))
    doc.close()
    return extract_framework_sections_from_text("\n".join(full_text))


class PDFSectionExtractor:
    def __init__(self, *, email: str, requests_per_second: float = 1.0, timeout_seconds: int = 60) -> None:
        self._limiter = PoliteRateLimiter(requests_per_second)
        self._client = httpx.AsyncClient(
            timeout=timeout_seconds,
            follow_redirects=True,
            headers={
                "User-Agent": f"SciSieve-MLR/1.0 (mailto:{email})",
                "Accept": "application/pdf,*/*",
            },
        )

    async def __aenter__(self) -> PDFSectionExtractor:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def _download_pdf(self, pdf_url: str, output_file: Path, max_retries: int = 4) -> bool:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(max_retries):
            await self._limiter.wait()
            try:
                response = await self._client.get(pdf_url)
                if response.status_code in (429, 500, 502, 503, 504):
                    await asyncio.sleep(min(2**attempt, 20))
                    continue
                response.raise_for_status()
                content_type = response.headers.get("content-type", "").lower()
                if "pdf" not in content_type and not pdf_url.lower().endswith(".pdf"):
                    if response.content[:4] != b"%PDF":
                        return False
                output_file.write_bytes(response.content)
                return True
            except httpx.HTTPError:
                if attempt >= max_retries - 1:
                    return False
                await asyncio.sleep(min(2**attempt, 20))
        return False

    async def process_work(self, work: Mapping[str, Any], output_dir: Path) -> dict[str, Any]:
        work_id = str(work.get("id", "unknown"))
        safe_work_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", work_id)
        pdf_url = best_pdf_url_from_work(work)
        if not pdf_url:
            return {
                "id": work_id,
                "title": work.get("title", ""),
                "doi": work.get("doi"),
                "status": "skipped_no_pdf_url",
                "framework_sections": {},
                "pdf_path": None,
            }

        pdf_path = output_dir / f"{safe_work_id}.pdf"
        downloaded = await self._download_pdf(pdf_url, pdf_path)
        if not downloaded:
            return {
                "id": work_id,
                "title": work.get("title", ""),
                "doi": work.get("doi"),
                "status": "download_failed",
                "framework_sections": {},
                "pdf_path": str(pdf_path),
            }

        try:
            sections = await asyncio.to_thread(extract_framework_sections_from_pdf, pdf_path)
            return {
                "id": work_id,
                "title": work.get("title", ""),
                "doi": work.get("doi"),
                "status": "parsed",
                "framework_sections": sections,
                "pdf_path": str(pdf_path),
            }
        except Exception as exc:
            return {
                "id": work_id,
                "title": work.get("title", ""),
                "doi": work.get("doi"),
                "status": f"parse_failed:{exc.__class__.__name__}",
                "framework_sections": {},
                "pdf_path": str(pdf_path),
            }

    async def process_many(
        self,
        works: Sequence[Mapping[str, Any]],
        output_dir: Path,
        *,
        max_concurrency: int = 3,
    ) -> list[dict[str, Any]]:
        semaphore = asyncio.Semaphore(max_concurrency)

        async def one(work: Mapping[str, Any]) -> dict[str, Any]:
            async with semaphore:
                return await self.process_work(work, output_dir)

        return await asyncio.gather(*(one(work) for work in works))


