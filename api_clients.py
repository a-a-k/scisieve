from __future__ import annotations

import asyncio
import json
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

import httpx

try:
    from scisieve import __version__ as SCISIEVE_VERSION
except Exception:
    SCISIEVE_VERSION = "dev"


class PoliteRateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be > 0")
        self._interval = 1.0 / requests_per_second
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                await asyncio.sleep(self._next_allowed - now)
                now = time.monotonic()
            self._next_allowed = now + self._interval


class BaseApiClient:
    def __init__(
        self,
        *,
        base_url: str,
        email: str,
        api_key: str = "",
        requests_per_second: float,
        timeout_seconds: float = 45.0,
        max_retries: int = 5,
    ) -> None:
        self.email = email
        self.api_key = api_key.strip()
        self.max_retries = max_retries
        self._limiter = PoliteRateLimiter(requests_per_second)
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout_seconds,
            follow_redirects=True,
            headers={
                "User-Agent": f"SciSieve-MLR/{SCISIEVE_VERSION} (mailto:{email})",
                "Accept": "application/json",
            },
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> BaseApiClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    @staticmethod
    def _next_midnight_utc_iso() -> str:
        now = datetime.now(tz=timezone.utc)
        next_midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
        return next_midnight.isoformat()

    def _raise_if_budget_exceeded(self, response: httpx.Response) -> None:
        if response.status_code != 429:
            return
        body_text = response.text or ""
        payload: dict[str, Any] = {}
        try:
            parsed = response.json()
            if isinstance(parsed, dict):
                payload = parsed
        except (ValueError, json.JSONDecodeError):
            payload = {}
        message = str(payload.get("message") or body_text)
        if "insufficient budget" not in message.lower():
            return
        cost_match = re.search(r"costs \$([0-9.]+)", message, flags=re.IGNORECASE)
        remaining_match = re.search(r"you only have \$([0-9.]+)", message, flags=re.IGNORECASE)
        reset_at = self._next_midnight_utc_iso() if "midnight utc" in message.lower() else ""
        raise OpenAlexBudgetExceeded(
            message=message.strip() or "OpenAlex daily budget exhausted.",
            reset_at_utc=reset_at,
            request_cost_usd=cost_match.group(1) if cost_match else "",
            remaining_budget_usd=remaining_match.group(1) if remaining_match else "",
        )

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        params = params or {}
        for attempt in range(self.max_retries):
            await self._limiter.wait()
            try:
                response = await self._client.request(method, url, params=params)
                self._raise_if_budget_exceeded(response)
                if response.status_code in (429, 500, 502, 503, 504):
                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        delay = float(retry_after)
                    else:
                        delay = min(2**attempt, 20) + random.uniform(0.1, 0.5)
                    await asyncio.sleep(delay)
                    continue
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError):
                if attempt >= self.max_retries - 1:
                    raise
                await asyncio.sleep(min(2**attempt, 20) + random.uniform(0.1, 0.5))
        raise RuntimeError("Request failed after retries")


class OpenAlexBudgetExceeded(RuntimeError):
    def __init__(
        self,
        *,
        message: str,
        reset_at_utc: str = "",
        request_cost_usd: str = "",
        remaining_budget_usd: str = "",
    ) -> None:
        super().__init__(message)
        self.reset_at_utc = reset_at_utc
        self.request_cost_usd = request_cost_usd
        self.remaining_budget_usd = remaining_budget_usd


class OpenAlexClient(BaseApiClient):
    def __init__(self, *, email: str, requests_per_second: float = 7.0, api_key: str = "") -> None:
        super().__init__(
            base_url="https://api.openalex.org",
            email=email,
            api_key=api_key,
            requests_per_second=requests_per_second,
        )

    def auth_params(self, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"mailto": self.email}
        if self.api_key:
            params["api_key"] = self.api_key
        if extra:
            params.update(extra)
        return params

    @staticmethod
    def clamp_per_page(per_page: int) -> int:
        return max(1, min(int(per_page), 100))

    @staticmethod
    def build_filter(
        *,
        types: Sequence[str],
        start_date: str,
        end_date: str,
        require_doi: bool = False,
        has_abstract: bool | None = None,
    ) -> str:
        clauses = [
            f"from_publication_date:{start_date}",
            f"to_publication_date:{end_date}",
            f"type:{'|'.join(types)}",
        ]
        if require_doi:
            clauses.append("has_doi:true")
        if has_abstract is True:
            clauses.append("has_abstract:true")
        if has_abstract is False:
            clauses.append("has_abstract:false")
        return ",".join(clauses)

    async def fetch_works(
        self,
        *,
        filter_expression: str,
        search_query: str | None = None,
        per_page: int = 100,
        max_records: int | None = None,
    ) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        cursor = "*"
        page_size = self.clamp_per_page(per_page)
        while True:
            params = self.auth_params({
                "filter": filter_expression,
                "per_page": page_size,
                "cursor": cursor,
            })
            if search_query:
                params["search"] = search_query

            payload = await self._request_json("GET", "/works", params=params)
            results = payload.get("results", [])
            if not isinstance(results, list) or not results:
                break

            collected.extend(result for result in results if isinstance(result, dict))
            if max_records is not None and len(collected) >= max_records:
                return collected[:max_records]

            cursor = payload.get("meta", {}).get("next_cursor")
            if not cursor:
                break
        return collected

    async def get_work(self, work_id_or_url: str) -> dict[str, Any]:
        target = work_id_or_url.strip()
        if not target:
            return {}
        normalized = target
        if target.startswith("http://") or target.startswith("https://"):
            if target.startswith("https://openalex.org/") or target.startswith("http://openalex.org/"):
                normalized = target.rstrip("/").rsplit("/", 1)[-1]
            elif target.startswith("https://api.openalex.org/works/") or target.startswith("http://api.openalex.org/works/"):
                normalized = target.rstrip("/").rsplit("/", 1)[-1]
            else:
                payload = await self._request_json("GET", target, params=self.auth_params())
                return payload if isinstance(payload, dict) else {}
        if normalized.upper().startswith("W") and normalized[1:].isdigit():
            normalized = normalized.upper()
        payload = await self._request_json("GET", f"/works/{normalized}", params=self.auth_params())
        return payload if isinstance(payload, dict) else {}

    async def fetch_by_ids(self, ids: Sequence[str], *, max_concurrency: int = 10) -> list[dict[str, Any]]:
        semaphore = asyncio.Semaphore(max_concurrency)

        async def one(work_id: str) -> dict[str, Any] | None:
            async with semaphore:
                try:
                    return await self.get_work(work_id)
                except Exception:
                    return None

        tasks = [one(work_id) for work_id in ids if work_id]
        results = await asyncio.gather(*tasks)
        return [result for result in results if result]

    async def fetch_citing_works(
        self,
        cited_by_api_url: str,
        *,
        per_page: int = 100,
        max_records: int | None = None,
    ) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        cursor = "*"
        page_size = self.clamp_per_page(per_page)
        while True:
            params = self.auth_params({"per_page": page_size, "cursor": cursor})
            payload = await self._request_json("GET", cited_by_api_url, params=params)
            results = payload.get("results", [])
            if not isinstance(results, list) or not results:
                break
            collected.extend(result for result in results if isinstance(result, dict))
            if max_records is not None and len(collected) >= max_records:
                return collected[:max_records]
            cursor = payload.get("meta", {}).get("next_cursor")
            if not cursor:
                break
        return collected

    async def lookup_work_by_doi(self, doi: str) -> dict[str, Any] | None:
        normalized = doi.strip().lower()
        if not normalized:
            return None
        filter_expression = f"doi:{normalized}"
        results = await self.fetch_works(
            filter_expression=filter_expression,
            per_page=1,
            max_records=1,
        )
        return results[0] if results else None


class OpenCitationsClient(BaseApiClient):
    def __init__(self, *, email: str = "", requests_per_second: float = 3.0, access_token: str = "") -> None:
        super().__init__(
            base_url="https://api.opencitations.net/index/v2",
            email=email or "noreply@example.org",
            requests_per_second=requests_per_second,
        )
        if access_token:
            self._client.headers["authorization"] = access_token

    @staticmethod
    def _normalize_doi(raw_doi: str) -> str:
        value = raw_doi.strip()
        value = value.removeprefix("doi:")
        value = value.removeprefix("https://doi.org/")
        value = value.removeprefix("http://doi.org/")
        return value.lower()

    @classmethod
    def extract_doi_from_pid_field(cls, pid_field: str) -> str | None:
        for token in pid_field.split():
            if token.lower().startswith("doi:"):
                normalized = cls._normalize_doi(token)
                return normalized or None
        return None

    @staticmethod
    def extract_openalex_id_from_pid_field(pid_field: str) -> str | None:
        for token in pid_field.split():
            if token.lower().startswith("openalex:"):
                value = token.split(":", 1)[1].strip()
                if value.upper().startswith("W") and value[1:].isdigit():
                    return f"https://openalex.org/{value.upper()}"
        return None

    async def _fetch_doi_relations(self, endpoint: str, doi: str) -> list[dict[str, Any]]:
        normalized = self._normalize_doi(doi)
        if not normalized:
            return []
        payload = await self._request_json("GET", f"/{endpoint}/doi:{normalized}", params={"format": "json"})
        if not isinstance(payload, list):
            return []
        return [row for row in payload if isinstance(row, dict)]

    async def fetch_references(self, doi: str) -> list[dict[str, Any]]:
        return await self._fetch_doi_relations("references", doi)

    async def fetch_citations(self, doi: str) -> list[dict[str, Any]]:
        return await self._fetch_doi_relations("citations", doi)

