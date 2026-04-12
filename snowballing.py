from __future__ import annotations

import asyncio
from typing import Any, Sequence

from api_clients import OpenAlexClient, OpenCitationsClient


def select_top_tier_seed_ids(formal_stream_works: Sequence[dict[str, Any]], n: int = 25) -> list[str]:
    ranked = sorted(
        (work for work in formal_stream_works if isinstance(work, dict)),
        key=lambda work: int(work.get("cited_by_count") or 0),
        reverse=True,
    )
    selected_ids: list[str] = []
    for work in ranked:
        work_id = work.get("id")
        if not work_id:
            continue
        selected_ids.append(work_id)
        if len(selected_ids) >= n:
            break
    return selected_ids


async def backward_snowball(
    openalex: OpenAlexClient,
    seed_ids: Sequence[str],
    *,
    max_refs_per_seed: int = 250,
    max_total_refs: int = 3000,
) -> list[dict[str, Any]]:
    seed_works = await openalex.fetch_by_ids(seed_ids)
    ref_ids: list[str] = []
    for seed in seed_works:
        refs = seed.get("referenced_works", [])
        if not isinstance(refs, list):
            continue
        for ref_id in refs[:max_refs_per_seed]:
            if isinstance(ref_id, str):
                ref_ids.append(ref_id)
            if len(ref_ids) >= max_total_refs:
                break
        if len(ref_ids) >= max_total_refs:
            break

    unique_ref_ids = list(dict.fromkeys(ref_ids))
    return await openalex.fetch_by_ids(unique_ref_ids)


async def forward_snowball(
    openalex: OpenAlexClient,
    seed_ids: Sequence[str],
    *,
    max_records_per_seed: int = 300,
) -> list[dict[str, Any]]:
    seed_works = await openalex.fetch_by_ids(seed_ids)
    cited_batches = []
    for seed in seed_works:
        cited_by_url = seed.get("cited_by_api_url")
        if not cited_by_url:
            continue
        cited_batches.append(
            openalex.fetch_citing_works(
                cited_by_url,
                max_records=max_records_per_seed,
            )
        )

    if not cited_batches:
        return []

    gathered = await asyncio.gather(*cited_batches, return_exceptions=True)
    merged: list[dict[str, Any]] = []
    for batch in gathered:
        if isinstance(batch, Exception):
            continue
        merged.extend(batch)

    deduped: dict[str, dict[str, Any]] = {}
    for work in merged:
        work_id = work.get("id")
        if work_id and work_id not in deduped:
            deduped[work_id] = work
    return list(deduped.values())


async def backward_snowball_opencitations(
    openalex: OpenAlexClient,
    opencitations: OpenCitationsClient,
    seed_dois: Sequence[str],
    *,
    max_refs_per_seed: int = 250,
) -> list[dict[str, Any]]:
    cited_openalex_ids: list[str] = []
    cited_dois: list[str] = []
    for seed_doi in seed_dois:
        rows = await opencitations.fetch_references(seed_doi)
        for row in rows[:max_refs_per_seed]:
            cited_field = str(row.get("cited") or "").strip()
            openalex_id = opencitations.extract_openalex_id_from_pid_field(cited_field)
            if openalex_id:
                cited_openalex_ids.append(openalex_id)
                continue
            doi = opencitations.extract_doi_from_pid_field(cited_field)
            if doi:
                cited_dois.append(doi)
    resolved_ids = await openalex.fetch_by_ids(list(dict.fromkeys(cited_openalex_ids)))
    resolved_dois = await asyncio.gather(
        *(openalex.lookup_work_by_doi(doi) for doi in list(dict.fromkeys(cited_dois))),
        return_exceptions=True,
    )
    merged = [row for row in resolved_ids if isinstance(row, dict) and row]
    merged.extend(row for row in resolved_dois if isinstance(row, dict) and row)
    deduped: dict[str, dict[str, Any]] = {}
    for row in merged:
        work_id = str(row.get("id") or "")
        if work_id and work_id not in deduped:
            deduped[work_id] = row
    return list(deduped.values())


async def forward_snowball_opencitations(
    openalex: OpenAlexClient,
    opencitations: OpenCitationsClient,
    seed_dois: Sequence[str],
    *,
    max_records_per_seed: int = 300,
) -> list[dict[str, Any]]:
    citing_openalex_ids: list[str] = []
    citing_dois: list[str] = []
    for seed_doi in seed_dois:
        rows = await opencitations.fetch_citations(seed_doi)
        for row in rows[:max_records_per_seed]:
            citing_field = str(row.get("citing") or "").strip()
            openalex_id = opencitations.extract_openalex_id_from_pid_field(citing_field)
            if openalex_id:
                citing_openalex_ids.append(openalex_id)
                continue
            doi = opencitations.extract_doi_from_pid_field(citing_field)
            if doi:
                citing_dois.append(doi)
    resolved_ids = await openalex.fetch_by_ids(list(dict.fromkeys(citing_openalex_ids)))
    resolved_dois = await asyncio.gather(
        *(openalex.lookup_work_by_doi(doi) for doi in list(dict.fromkeys(citing_dois))),
        return_exceptions=True,
    )
    merged = [row for row in resolved_ids if isinstance(row, dict) and row]
    merged.extend(row for row in resolved_dois if isinstance(row, dict) and row)
    deduped: dict[str, dict[str, Any]] = {}
    for row in merged:
        work_id = str(row.get("id") or "")
        if work_id and work_id not in deduped:
            deduped[work_id] = row
    return list(deduped.values())
