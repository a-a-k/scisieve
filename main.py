from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import re
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from api_clients import OpenAlexClient
from extractor import PDFSectionExtractor, best_pdf_url_from_work
from models import (
    ResearchWork,
    infer_cloud_context,
    infer_resilience_paradigm,
    normalize_doi,
    reconstruct_abstract,
)
from protocol import (
    CANONICAL_SEARCH_STRING,
    NEGATIVE_EXCLUSION_TERMS,
    build_openalex_query_from_expanded,
    build_screening_text,
    compile_group_regex,
    compile_negative_regex,
    evaluate_protocol,
    expand_terms_for_openalex,
    format_search_strings_report,
)
from snowballing import backward_snowball, forward_snowball, select_top_tier_seed_ids


TOOL_VERSION = "1.2.0"
DEFAULT_START_DATE = "2010-01-01"
PREPRINT_WATCHLIST_TAG = "Candidate for Preprint Watchlist (trends-only)"

SCREENING_COLUMNS = ["source", "title", "doi", "type", "reason_for_exclusion", "screening_stage"]
METADATA_COLUMNS = [
    "source",
    "track",
    "archival_status",
    "is_in_core_corpus",
    "replaced_by_doi",
    "matched_archival_id",
    "matched_archival_doi",
    "openalex_id",
    "title",
    "doi",
    "type",
    "publication_date",
    "publication_year",
    "is_watchlist_candidate",
    "watchlist_tag",
    "resilience_paradigm",
    "cloud_context",
    "cited_by_count",
    "best_pdf_url",
]
SNOWBALL_COLUMNS = ["id", "title", "doi", "type", "publication_date", "cited_by_count"]
PREPRINT_RESOLUTION_COLUMNS = [
    "preprint_id",
    "preprint_doi",
    "preprint_title",
    "matched_core_id",
    "matched_core_doi",
    "similarity",
    "method",
    "action",
]
DEDUP_SUMMARY_COLUMNS = [
    "raw_core_retrieved",
    "raw_preprint_retrieved",
    "included_topical_core_raw",
    "included_topical_preprint_raw",
    "core_duplicates_removed_by_doi_or_title",
    "preprint_duplicates_removed_by_doi_or_title",
    "preprint_duplicates_removed_against_core",
    "preprints_resolved_to_existing_core",
    "preprints_resolved_to_new_archival",
    "final_core_count",
    "final_preprint_watchlist_count",
]
PRISMA_STAGE_ORDER = [
    "retrieved_core_raw",
    "retrieved_preprint_raw",
    "included_topical_core_raw",
    "included_topical_preprint_raw",
    "core_after_dedup",
    "preprint_watchlist_after_dedup",
    "preprints_resolved_to_existing_core",
    "preprints_resolved_to_new_archival",
    "final_export_core",
    "final_export_preprint_watchlist",
]
SCREENING_TEMPLATE_COLUMNS = [
    "source",
    "openalex_id",
    "title",
    "doi",
    "type",
    "publication_date",
    "publication_year",
    "reviewer1_decision",
    "reviewer1_reason",
    "reviewer2_decision",
    "reviewer2_reason",
    "resolved_decision",
    "resolved_reason",
    "notes",
]
QUALITY_APPRAISAL_COLUMNS = [
    "source",
    "openalex_id",
    "title",
    "doi",
    "type",
    "publication_date",
    "publication_year",
    "reviewer1_Q1",
    "reviewer1_Q2",
    "reviewer1_Q3",
    "reviewer1_Q4",
    "reviewer1_Q5",
    "reviewer2_Q1",
    "reviewer2_Q2",
    "reviewer2_Q3",
    "reviewer2_Q4",
    "reviewer2_Q5",
    "resolved_Q1",
    "resolved_Q2",
    "resolved_Q3",
    "resolved_Q4",
    "resolved_Q5",
    "notes",
]


@dataclass
class DedupStats:
    core_works: list[ResearchWork]
    watchlist_works: list[ResearchWork]
    core_duplicates_removed_by_doi_or_title: int
    preprint_duplicates_removed_by_doi_or_title: int
    preprint_duplicates_removed_against_core: int


@dataclass
class ResolutionResult:
    core_works: list[ResearchWork]
    watchlist_works: list[ResearchWork]
    resolution_rows: list[dict[str, Any]]
    resolved_to_existing_core: int
    resolved_to_new_archival: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OpenAlex retrieval pipeline for cloud resilience/dependability.")
    parser.add_argument("--email", required=True, help="Email used in polite OpenAlex User-Agent.")
    parser.add_argument("--output-dir", default="output", help="Directory for CSV/JSON outputs.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Start date (YYYY-MM-DD).")
    parser.add_argument(
        "--end-date",
        default="",
        help="End date (YYYY-MM-DD). Defaults to the current UTC date at runtime.",
    )

    parser.add_argument("--per-page", type=int, default=200, help="OpenAlex page size (max 200).")
    parser.add_argument("--max-formal-records", type=int, default=2000, help="Upper bound for core stream records.")
    parser.add_argument(
        "--max-watchlist-records",
        type=int,
        default=1200,
        help="Upper bound for preprint watchlist stream records.",
    )
    parser.add_argument("--openalex-rps", type=float, default=7.0, help="OpenAlex requests/second.")
    parser.add_argument(
        "--double-screen-fraction",
        type=float,
        default=0.2,
        help="Fraction of metadata exported for double-screening reliability checks.",
    )
    parser.add_argument("--double-screen-seed", type=int, default=42, help="Random seed for double-screening sample.")

    parser.add_argument("--run-snowballing", action="store_true", help="Run backward and forward snowballing.")
    parser.add_argument("--seed-count", type=int, default=25, help="Top-cited seed count if --seed-file is not set.")
    parser.add_argument("--seed-file", default="", help="Path to seed list (OpenAlex IDs/URLs or DOIs), one per line.")
    parser.add_argument("--backward-refs-per-seed", type=int, default=250, help="Max references per seed in backward.")
    parser.add_argument("--backward-max-total", type=int, default=3000, help="Global cap for backward references.")
    parser.add_argument("--forward-max-per-seed", type=int, default=300, help="Max citing works per seed in forward.")
    parser.add_argument(
        "--merge-snowball-into-core",
        action="store_true",
        help="If set, merge screened snowball candidates into the final core export.",
    )

    parser.add_argument(
        "--resolve-preprints",
        action="store_true",
        help="Enable title-search resolution to replace watchlist preprints with archival matches.",
    )
    parser.add_argument(
        "--resolve-preprints-threshold",
        type=float,
        default=0.92,
        help="Similarity threshold for OpenAlex title-search preprint replacement.",
    )

    parser.add_argument("--download-pdfs", action="store_true", help="Download and parse PDFs via best_oa_location.")
    parser.add_argument("--pdf-limit", type=int, default=50, help="Max works to process for PDF extraction.")
    parser.add_argument("--pdf-rps", type=float, default=1.0, help="PDF download requests/second.")
    parser.add_argument("--pdf-concurrency", type=int, default=3, help="Concurrent PDF processing workers.")
    return parser.parse_args()


def _parse_cli_date(raw_value: str, *, arg_name: str) -> date:
    try:
        return date.fromisoformat(raw_value)
    except ValueError as exc:
        raise ValueError(f"{arg_name} must be YYYY-MM-DD; got: {raw_value}") from exc


def _default_end_date(*, now_provider: Callable[[], datetime] | None = None) -> date:
    provider = now_provider or (lambda: datetime.now(tz=timezone.utc))
    return provider().date()


def _resolve_end_date(
    raw_value: str | None,
    *,
    now_provider: Callable[[], datetime] | None = None,
) -> date:
    if raw_value and raw_value.strip():
        return _parse_cli_date(raw_value, arg_name="--end-date")
    return _default_end_date(now_provider=now_provider)


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _build_run_config(
    args: argparse.Namespace,
    *,
    start_date: date,
    end_date: date,
    run_timestamp_utc: str,
) -> dict[str, Any]:
    resolved_args = vars(args).copy()
    resolved_args["start_date"] = start_date.isoformat()
    resolved_args["end_date"] = end_date.isoformat()
    return {
        "tool_version": TOOL_VERSION,
        "git_commit_hash": _best_effort_git_hash(),
        "run_timestamp_utc": run_timestamp_utc,
        "args": resolved_args,
    }


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _parse_work_date(work: Mapping[str, Any]) -> date | None:
    publication_date = work.get("publication_date")
    if isinstance(publication_date, str) and publication_date:
        try:
            return date.fromisoformat(publication_date)
        except ValueError:
            pass
    publication_year = work.get("publication_year")
    if isinstance(publication_year, int):
        try:
            return date(publication_year, 1, 1)
        except ValueError:
            return None
    return None


def _within_time_window(work: Mapping[str, Any], *, start_date: date, end_date: date) -> bool:
    parsed = _parse_work_date(work)
    if parsed is None:
        return False
    return start_date <= parsed <= end_date


def _is_archival_work_type(raw_type: str | None) -> bool:
    return str(raw_type or "").strip().lower() in {"article", "proceedings-article"}


def is_archival(work: Mapping[str, Any] | ResearchWork) -> bool:
    if isinstance(work, ResearchWork):
        return _is_archival_work_type(work.work_type)
    return _is_archival_work_type(work.get("type"))


def _screen_work(
    work: Mapping[str, Any],
    *,
    stream_source: str,
    is_preprint_stream: bool,
    start_date: date,
    end_date: date,
    compiled_groups: Mapping[str, Sequence[re.Pattern[str]]],
    compiled_exclusions: Sequence[re.Pattern[str]],
) -> tuple[bool, dict[str, Any]]:
    row = {
        "source": stream_source,
        "title": str(work.get("title") or ""),
        "doi": normalize_doi(work.get("doi")) or "",
        "type": str(work.get("type") or ""),
        "reason_for_exclusion": "",
        "screening_stage": "",
    }

    if not _within_time_window(work, start_date=start_date, end_date=end_date):
        row["reason_for_exclusion"] = "outside_time_window"
        row["screening_stage"] = "date_filter"
        return False, row

    work_type = str(work.get("type") or "").strip().lower()
    if is_preprint_stream:
        if work_type != "preprint":
            row["reason_for_exclusion"] = "type_not_preprint"
            row["screening_stage"] = "type_filter"
            return False, row
    elif not is_archival(work):
        row["reason_for_exclusion"] = "not_archival_type"
        row["screening_stage"] = "type_filter"
        return False, row

    matched, reason = evaluate_protocol(
        build_screening_text(work),
        compiled_groups=compiled_groups,
        compiled_exclusions=compiled_exclusions,
    )
    if not matched:
        row["reason_for_exclusion"] = reason or "failed_protocol_match"
        row["screening_stage"] = "negative_domain_filter" if reason == "negative_domain_exclusion" else "title_abstract_filter"
        return False, row

    row["screening_stage"] = "included_topical_preprint_watchlist" if is_preprint_stream else "included_topical_core"
    return True, row


def _to_research_work(
    work: Mapping[str, Any],
    *,
    source: str,
    track: str,
    resolved_from_preprint: bool = False,
    matched_archival_id: str | None = None,
    matched_archival_doi: str | None = None,
) -> ResearchWork:
    abstract = reconstruct_abstract(work.get("abstract_inverted_index"))
    screening_text = build_screening_text(work)
    openalex_id = str(work.get("id")) if work.get("id") else None
    doi = normalize_doi(work.get("doi"))
    is_core = track == "core"
    return ResearchWork(
        source=source,
        openalex_id=openalex_id,
        title=str(work.get("title") or ""),
        doi=doi,
        type=str(work.get("type") or ""),
        publication_date=work.get("publication_date"),
        publication_year=work.get("publication_year"),
        track=track,
        archival_status="archival" if is_archival(work) else "preprint_watchlist",
        is_in_core_corpus=is_core,
        is_watchlist_candidate=not is_core,
        watchlist_tag=PREPRINT_WATCHLIST_TAG if not is_core else None,
        matched_archival_id=matched_archival_id or (openalex_id if resolved_from_preprint else None),
        matched_archival_doi=matched_archival_doi or (doi if resolved_from_preprint else None),
        resolved_from_preprint=resolved_from_preprint,
        resilience_paradigm=infer_resilience_paradigm(screening_text),
        cloud_context=infer_cloud_context(screening_text),
        cited_by_count=work.get("cited_by_count"),
        best_pdf_url=best_pdf_url_from_work(work),
        abstract=abstract,
    )


def _date_sort_key(work: ResearchWork) -> int:
    if work.publication_date:
        try:
            return date.fromisoformat(work.publication_date).toordinal()
        except ValueError:
            pass
    if isinstance(work.publication_year, int):
        try:
            return date(work.publication_year, 1, 1).toordinal()
        except ValueError:
            pass
    return 0


def _abstract_richness(work: ResearchWork) -> int:
    return len((work.abstract or "").split())


def _choose_preferred_work(left: ResearchWork, right: ResearchWork) -> ResearchWork:
    def ranking(work: ResearchWork) -> tuple[int, int, int, int]:
        if is_archival(work):
            archival_rank = 1 if work.resolved_from_preprint else 2
        else:
            archival_rank = 0
        return (
            archival_rank,
            _abstract_richness(work),
            int(work.cited_by_count or 0),
            _date_sort_key(work),
        )

    left_rank = ranking(left)
    right_rank = ranking(right)
    if left_rank > right_rank:
        return left
    if right_rank > left_rank:
        return right
    left_id = (left.openalex_id or "").lower()
    right_id = (right.openalex_id or "").lower()
    return left if left_id <= right_id else right


def _deduplicate_works(works: Sequence[ResearchWork]) -> tuple[list[ResearchWork], int]:
    by_doi: dict[str, ResearchWork] = {}
    ordered_dois: list[str] = []
    without_doi: list[ResearchWork] = []
    removed_by_doi = 0

    for work in works:
        if work.doi:
            existing = by_doi.get(work.doi)
            if existing is None:
                by_doi[work.doi] = work
                ordered_dois.append(work.doi)
            else:
                by_doi[work.doi] = _choose_preferred_work(existing, work)
                removed_by_doi += 1
            continue
        without_doi.append(work)

    after_doi = [by_doi[doi] for doi in ordered_dois] + without_doi

    by_title: dict[str, ResearchWork] = {}
    ordered_titles: list[str] = []
    without_title: list[ResearchWork] = []
    removed_by_title = 0

    for work in after_doi:
        title_key = _normalize_title_for_match(work.title)
        if not title_key:
            without_title.append(work)
            continue
        existing = by_title.get(title_key)
        if existing is None:
            by_title[title_key] = work
            ordered_titles.append(title_key)
        else:
            by_title[title_key] = _choose_preferred_work(existing, work)
            removed_by_title += 1

    deduped = [by_title[title_key] for title_key in ordered_titles] + without_title
    return deduped, removed_by_doi + removed_by_title


def _canonical_keys(work: ResearchWork) -> list[tuple[str, str]]:
    keys: list[tuple[str, str]] = []
    if work.doi:
        keys.append(("doi", work.doi))
    title_key = _normalize_title_for_match(work.title)
    if title_key:
        keys.append(("title", title_key))
    return keys


def _build_canonical_index(works: Sequence[ResearchWork]) -> dict[tuple[str, str], ResearchWork]:
    index: dict[tuple[str, str], ResearchWork] = {}
    for work in works:
        for key in _canonical_keys(work):
            existing = index.get(key)
            index[key] = work if existing is None else _choose_preferred_work(existing, work)
    return index


def _find_matching_work(
    work: ResearchWork,
    index: Mapping[tuple[str, str], ResearchWork],
) -> ResearchWork | None:
    for key in _canonical_keys(work):
        existing = index.get(key)
        if existing is not None:
            return existing
    return None


def _deduplicate_tracks(core_items: Sequence[ResearchWork], watchlist_items: Sequence[ResearchWork]) -> DedupStats:
    core_deduped, core_removed = _deduplicate_works(core_items)
    watchlist_deduped, watchlist_removed = _deduplicate_works(watchlist_items)

    core_index = _build_canonical_index(core_deduped)
    filtered_watchlist: list[ResearchWork] = []
    removed_against_core = 0
    for watch in watchlist_deduped:
        matched = _find_matching_work(watch, core_index)
        if matched is not None:
            watch.replaced_by_doi = matched.doi
            watch.matched_archival_id = matched.openalex_id
            watch.matched_archival_doi = matched.doi
            removed_against_core += 1
            continue
        filtered_watchlist.append(watch)

    return DedupStats(
        core_works=core_deduped,
        watchlist_works=filtered_watchlist,
        core_duplicates_removed_by_doi_or_title=core_removed,
        preprint_duplicates_removed_by_doi_or_title=watchlist_removed,
        preprint_duplicates_removed_against_core=removed_against_core,
    )


def _normalize_title_for_match(title: str) -> str:
    lowered = title.lower()
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


async def _search_archival_match_by_title(
    openalex: OpenAlexClient,
    title: str,
    *,
    start_date: str,
    end_date: str,
    threshold: float,
    compiled_groups: Mapping[str, Sequence[re.Pattern[str]]],
    compiled_exclusions: Sequence[re.Pattern[str]],
) -> tuple[dict[str, Any] | None, float]:
    title_norm = _normalize_title_for_match(title)
    if not title_norm:
        return None, 0.0

    filter_expression = OpenAlexClient.build_filter(
        types=["article", "proceedings-article"],
        start_date=start_date,
        end_date=end_date,
        require_doi=False,
    )
    candidates = await openalex.fetch_works(
        filter_expression=filter_expression,
        search_query=title,
        per_page=15,
        max_records=15,
    )

    best_candidate: dict[str, Any] | None = None
    best_similarity = 0.0
    for candidate in candidates:
        if not is_archival(candidate):
            continue
        matched, _reason = evaluate_protocol(
            build_screening_text(candidate),
            compiled_groups=compiled_groups,
            compiled_exclusions=compiled_exclusions,
        )
        if not matched:
            continue
        candidate_norm = _normalize_title_for_match(str(candidate.get("title") or ""))
        if not candidate_norm:
            continue
        similarity = SequenceMatcher(None, title_norm, candidate_norm).ratio()
        if similarity > best_similarity:
            best_similarity = similarity
            best_candidate = candidate

    if best_candidate is not None and best_similarity >= threshold:
        return best_candidate, best_similarity
    return None, 0.0


async def _resolve_preprints_against_core(
    openalex: OpenAlexClient,
    *,
    core_works: Sequence[ResearchWork],
    watchlist_works: Sequence[ResearchWork],
    raw_by_openalex_id: dict[str, dict[str, Any]],
    start_date: str,
    end_date: str,
    resolve_preprints: bool,
    similarity_threshold: float,
    compiled_groups: Mapping[str, Sequence[re.Pattern[str]]],
    compiled_exclusions: Sequence[re.Pattern[str]],
) -> ResolutionResult:
    core_current = list(core_works)
    core_index = _build_canonical_index(core_current)
    unresolved_watchlist: list[ResearchWork] = []
    resolution_rows: list[dict[str, Any]] = []
    resolved_to_existing_core = 0
    resolved_to_new_archival = 0

    for preprint in watchlist_works:
        matched_core: ResearchWork | None = None
        method = ""
        action = ""
        similarity = 0.0

        normalized_title = _normalize_title_for_match(preprint.title)
        if normalized_title:
            matched_core = core_index.get(("title", normalized_title))
        if matched_core is not None:
            method = "exact_title"
            action = "matched_existing_core"
            similarity = 1.0
            resolved_to_existing_core += 1
        elif resolve_preprints and preprint.title.strip():
            matched_work, similarity = await _search_archival_match_by_title(
                openalex,
                preprint.title,
                start_date=start_date,
                end_date=end_date,
                threshold=similarity_threshold,
                compiled_groups=compiled_groups,
                compiled_exclusions=compiled_exclusions,
            )
            if matched_work is not None:
                matched_candidate = _to_research_work(
                    matched_work,
                    source="OpenAlex Title Resolution",
                    track="core",
                    resolved_from_preprint=True,
                )
                existing = _find_matching_work(matched_candidate, core_index)
                if existing is not None:
                    matched_core = existing
                    action = "matched_existing_core"
                    resolved_to_existing_core += 1
                else:
                    matched_core = matched_candidate
                    core_current.append(matched_candidate)
                    core_index = _build_canonical_index(core_current)
                    action = "added_new_archival_core"
                    resolved_to_new_archival += 1
                    if matched_candidate.openalex_id:
                        raw_by_openalex_id[matched_candidate.openalex_id] = matched_work
                method = "title_search_openalex"

        if matched_core is not None:
            preprint.replaced_by_doi = matched_core.doi
            preprint.matched_archival_id = matched_core.openalex_id
            preprint.matched_archival_doi = matched_core.doi
            resolution_rows.append(
                {
                    "preprint_id": preprint.openalex_id or "",
                    "preprint_doi": preprint.doi or "",
                    "preprint_title": preprint.title,
                    "matched_core_id": matched_core.openalex_id or "",
                    "matched_core_doi": matched_core.doi or "",
                    "similarity": f"{similarity:.4f}",
                    "method": method,
                    "action": action,
                }
            )
            continue

        unresolved_watchlist.append(preprint)

    core_final, _removed = _deduplicate_works(core_current)
    return ResolutionResult(
        core_works=core_final,
        watchlist_works=unresolved_watchlist,
        resolution_rows=resolution_rows,
        resolved_to_existing_core=resolved_to_existing_core,
        resolved_to_new_archival=resolved_to_new_archival,
    )


def _research_work_to_row(work: ResearchWork) -> dict[str, Any]:
    return {
        "source": work.source,
        "track": work.track,
        "archival_status": work.archival_status,
        "is_in_core_corpus": work.is_in_core_corpus,
        "replaced_by_doi": work.replaced_by_doi or "",
        "matched_archival_id": work.matched_archival_id or "",
        "matched_archival_doi": work.matched_archival_doi or "",
        "openalex_id": work.openalex_id or "",
        "title": work.title,
        "doi": work.doi or "",
        "type": work.work_type,
        "publication_date": work.publication_date or "",
        "publication_year": work.publication_year or "",
        "is_watchlist_candidate": work.is_watchlist_candidate,
        "watchlist_tag": work.watchlist_tag or "",
        "resilience_paradigm": work.resilience_paradigm.value,
        "cloud_context": work.cloud_context.value,
        "cited_by_count": work.cited_by_count if work.cited_by_count is not None else "",
        "best_pdf_url": work.best_pdf_url or "",
    }


def _openalex_row(work: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": work.get("id", ""),
        "title": work.get("title", ""),
        "doi": normalize_doi(work.get("doi")) or "",
        "type": work.get("type", ""),
        "publication_date": work.get("publication_date", ""),
        "cited_by_count": work.get("cited_by_count", ""),
    }


def _export_double_screen_sample(
    metadata_rows: Sequence[dict[str, Any]],
    output_file: Path,
    *,
    fraction: float,
    seed: int,
) -> int:
    if not metadata_rows:
        _write_csv(output_file, [], METADATA_COLUMNS)
        return 0
    sample_size = max(1, round(len(metadata_rows) * fraction))
    sample_size = min(sample_size, len(metadata_rows))
    rng = random.Random(seed)
    sample_rows = rng.sample(list(metadata_rows), sample_size)
    _write_csv(output_file, sample_rows, METADATA_COLUMNS)
    return sample_size


def _best_effort_git_hash() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except Exception:
        return ""


def _build_prisma_counts(
    *,
    raw_core_retrieved: int,
    raw_preprint_retrieved: int,
    included_topical_core_raw: int,
    included_topical_preprint_raw: int,
    core_after_dedup: int,
    preprint_watchlist_after_dedup: int,
    preprints_resolved_to_existing_core: int,
    preprints_resolved_to_new_archival: int,
    final_export_core: int,
    final_export_preprint_watchlist: int,
) -> dict[str, int]:
    return {
        "retrieved_core_raw": raw_core_retrieved,
        "retrieved_preprint_raw": raw_preprint_retrieved,
        "included_topical_core_raw": included_topical_core_raw,
        "included_topical_preprint_raw": included_topical_preprint_raw,
        "core_after_dedup": core_after_dedup,
        "preprint_watchlist_after_dedup": preprint_watchlist_after_dedup,
        "preprints_resolved_to_existing_core": preprints_resolved_to_existing_core,
        "preprints_resolved_to_new_archival": preprints_resolved_to_new_archival,
        "final_export_core": final_export_core,
        "final_export_preprint_watchlist": final_export_preprint_watchlist,
    }


def _build_dedup_summary(
    *,
    raw_core_retrieved: int,
    raw_preprint_retrieved: int,
    included_topical_core_raw: int,
    included_topical_preprint_raw: int,
    dedup_stats: DedupStats,
    resolution_result: ResolutionResult,
    final_core_count: int,
    final_preprint_watchlist_count: int,
) -> dict[str, int]:
    return {
        "raw_core_retrieved": raw_core_retrieved,
        "raw_preprint_retrieved": raw_preprint_retrieved,
        "included_topical_core_raw": included_topical_core_raw,
        "included_topical_preprint_raw": included_topical_preprint_raw,
        "core_duplicates_removed_by_doi_or_title": dedup_stats.core_duplicates_removed_by_doi_or_title,
        "preprint_duplicates_removed_by_doi_or_title": dedup_stats.preprint_duplicates_removed_by_doi_or_title,
        "preprint_duplicates_removed_against_core": dedup_stats.preprint_duplicates_removed_against_core,
        "preprints_resolved_to_existing_core": resolution_result.resolved_to_existing_core,
        "preprints_resolved_to_new_archival": resolution_result.resolved_to_new_archival,
        "final_core_count": final_core_count,
        "final_preprint_watchlist_count": final_preprint_watchlist_count,
    }


def _write_prisma_counts(output_file: Path, counts: Mapping[str, int]) -> None:
    rows = [{"stage": stage, "count": counts[stage]} for stage in PRISMA_STAGE_ORDER]
    _write_csv(output_file, rows, ["stage", "count"])


def _screening_template_rows(core_works: Sequence[ResearchWork]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for work in core_works:
        rows.append(
            {
                "source": work.source,
                "openalex_id": work.openalex_id or "",
                "title": work.title,
                "doi": work.doi or "",
                "type": work.work_type,
                "publication_date": work.publication_date or "",
                "publication_year": work.publication_year or "",
                "reviewer1_decision": "",
                "reviewer1_reason": "",
                "reviewer2_decision": "",
                "reviewer2_reason": "",
                "resolved_decision": "",
                "resolved_reason": "",
                "notes": "",
            }
        )
    return rows


def _quality_appraisal_rows(core_works: Sequence[ResearchWork]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for work in core_works:
        rows.append(
            {
                "source": work.source,
                "openalex_id": work.openalex_id or "",
                "title": work.title,
                "doi": work.doi or "",
                "type": work.work_type,
                "publication_date": work.publication_date or "",
                "publication_year": work.publication_year or "",
                "reviewer1_Q1": "",
                "reviewer1_Q2": "",
                "reviewer1_Q3": "",
                "reviewer1_Q4": "",
                "reviewer1_Q5": "",
                "reviewer2_Q1": "",
                "reviewer2_Q2": "",
                "reviewer2_Q3": "",
                "reviewer2_Q4": "",
                "reviewer2_Q5": "",
                "resolved_Q1": "",
                "resolved_Q2": "",
                "resolved_Q3": "",
                "resolved_Q4": "",
                "resolved_Q5": "",
                "notes": "",
            }
        )
    return rows


async def _seed_ids_from_file(openalex: OpenAlexClient, seed_file: Path) -> list[str]:
    if not seed_file.exists():
        return []

    openalex_refs: list[str] = []
    dois: list[str] = []
    for raw_line in seed_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "openalex.org/" in line.lower():
            openalex_refs.append(line)
            continue
        if re.fullmatch(r"[Ww]\d+", line):
            openalex_refs.append(f"https://openalex.org/{line.upper()}")
            continue
        doi = normalize_doi(line)
        if doi:
            dois.append(doi)

    semaphore = asyncio.Semaphore(8)

    async def lookup(doi: str) -> str | None:
        async with semaphore:
            work = await openalex.lookup_work_by_doi(doi)
            work_id = str(work.get("id") or "") if work else ""
            return work_id or None

    resolved_doi_ids = await asyncio.gather(*(lookup(doi) for doi in dois))
    for work_id in resolved_doi_ids:
        if work_id:
            openalex_refs.append(work_id)

    seen: set[str] = set()
    deduped: list[str] = []
    for item in openalex_refs:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _screen_snowball_work(
    work: Mapping[str, Any],
    *,
    start_date: date,
    end_date: date,
    compiled_groups: Mapping[str, Sequence[re.Pattern[str]]],
    compiled_exclusions: Sequence[re.Pattern[str]],
) -> tuple[bool, dict[str, Any]]:
    row = {
        "source": "OpenAlex Snowballing",
        "title": str(work.get("title") or ""),
        "doi": normalize_doi(work.get("doi")) or "",
        "type": str(work.get("type") or ""),
        "reason_for_exclusion": "",
        "screening_stage": "",
    }

    if not _within_time_window(work, start_date=start_date, end_date=end_date):
        row["reason_for_exclusion"] = "outside_time_window"
        row["screening_stage"] = "date_filter"
        return False, row

    if not is_archival(work):
        row["reason_for_exclusion"] = "not_archival_type"
        row["screening_stage"] = "type_filter"
        return False, row

    matched, reason = evaluate_protocol(
        build_screening_text(work),
        compiled_groups=compiled_groups,
        compiled_exclusions=compiled_exclusions,
    )
    if not matched:
        row["reason_for_exclusion"] = reason or "failed_protocol_match"
        row["screening_stage"] = "negative_domain_filter" if reason == "negative_domain_exclusion" else "title_abstract_filter"
        return False, row

    row["screening_stage"] = "included_topical_snowball_candidate"
    return True, row


def _map_research_works(works: Sequence[ResearchWork]) -> list[dict[str, Any]]:
    return [_research_work_to_row(work) for work in works]


async def run(args: argparse.Namespace) -> None:
    start_date = _parse_cli_date(args.start_date, arg_name="--start-date")
    end_date = _resolve_end_date(args.end_date)
    if end_date < start_date:
        raise ValueError("--end-date must be >= --start-date")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    run_timestamp_utc = datetime.now(tz=timezone.utc).isoformat()
    compiled_groups = compile_group_regex()
    compiled_exclusions = compile_negative_regex()
    expanded_terms = expand_terms_for_openalex()
    openalex_query = build_openalex_query_from_expanded(expanded_terms)

    search_strings_content = format_search_strings_report(
        canonical_search_string=CANONICAL_SEARCH_STRING,
        expanded_groups=expanded_terms,
        negative_exclusions=NEGATIVE_EXCLUSION_TERMS,
        openalex_query=openalex_query,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        run_timestamp_utc=run_timestamp_utc,
    )
    (output_dir / "search_strings.txt").write_text(search_strings_content, encoding="utf-8")
    _write_json(
        output_dir / "run_config.json",
        _build_run_config(
            args,
            start_date=start_date,
            end_date=end_date,
            run_timestamp_utc=run_timestamp_utc,
        ),
    )

    core_filter = OpenAlexClient.build_filter(
        types=["article", "proceedings-article"],
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        require_doi=True,
    )
    preprint_filter = OpenAlexClient.build_filter(
        types=["preprint"],
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        require_doi=False,
    )

    screening_rows: list[dict[str, Any]] = []
    raw_by_openalex_id: dict[str, dict[str, Any]] = {}

    async with OpenAlexClient(email=args.email, requests_per_second=args.openalex_rps) as openalex:
        core_task = openalex.fetch_works(
            filter_expression=core_filter,
            search_query=openalex_query,
            per_page=args.per_page,
            max_records=args.max_formal_records,
        )
        preprint_task = openalex.fetch_works(
            filter_expression=preprint_filter,
            search_query=openalex_query,
            per_page=args.per_page,
            max_records=args.max_watchlist_records,
        )
        core_raw, preprint_raw = await asyncio.gather(core_task, preprint_task)

        included_core_raw: list[dict[str, Any]] = []
        included_preprint_raw: list[dict[str, Any]] = []

        for work in core_raw:
            include, row = _screen_work(
                work,
                stream_source="OpenAlex Core Stream",
                is_preprint_stream=False,
                start_date=start_date,
                end_date=end_date,
                compiled_groups=compiled_groups,
                compiled_exclusions=compiled_exclusions,
            )
            screening_rows.append(row)
            if not include:
                continue
            included_core_raw.append(work)
            work_id = str(work.get("id") or "")
            if work_id:
                raw_by_openalex_id[work_id] = work

        for work in preprint_raw:
            include, row = _screen_work(
                work,
                stream_source="OpenAlex Preprint Stream",
                is_preprint_stream=True,
                start_date=start_date,
                end_date=end_date,
                compiled_groups=compiled_groups,
                compiled_exclusions=compiled_exclusions,
            )
            screening_rows.append(row)
            if not include:
                continue
            included_preprint_raw.append(work)

        core_models = [
            _to_research_work(work, source="OpenAlex Core Stream", track="core")
            for work in included_core_raw
        ]
        watchlist_models = [
            _to_research_work(work, source="OpenAlex Preprint Stream", track="preprint_watchlist")
            for work in included_preprint_raw
        ]

        dedup_stats = _deduplicate_tracks(core_models, watchlist_models)
        resolution_result = await _resolve_preprints_against_core(
            openalex,
            core_works=dedup_stats.core_works,
            watchlist_works=dedup_stats.watchlist_works,
            raw_by_openalex_id=raw_by_openalex_id,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            resolve_preprints=args.resolve_preprints,
            similarity_threshold=args.resolve_preprints_threshold,
            compiled_groups=compiled_groups,
            compiled_exclusions=compiled_exclusions,
        )
        final_stats = _deduplicate_tracks(resolution_result.core_works, resolution_result.watchlist_works)
        final_core = final_stats.core_works
        final_watchlist = final_stats.watchlist_works

        snowball_candidates_included: list[dict[str, Any]] = []
        if args.run_snowballing:
            if args.seed_file:
                seed_ids = await _seed_ids_from_file(openalex, Path(args.seed_file))
            else:
                seed_ids = select_top_tier_seed_ids(included_core_raw, n=args.seed_count)

            if seed_ids:
                backward_rows = await backward_snowball(
                    openalex,
                    seed_ids,
                    max_refs_per_seed=args.backward_refs_per_seed,
                    max_total_refs=args.backward_max_total,
                )
                forward_rows = await forward_snowball(
                    openalex,
                    seed_ids,
                    max_records_per_seed=args.forward_max_per_seed,
                )
                snowball_merged: dict[str, dict[str, Any]] = {}
                for work in [*backward_rows, *forward_rows]:
                    work_id = str(work.get("id") or "")
                    if work_id and work_id not in snowball_merged:
                        snowball_merged[work_id] = work

                snowball_screening_rows: list[dict[str, Any]] = []
                for work in snowball_merged.values():
                    include, row = _screen_snowball_work(
                        work,
                        start_date=start_date,
                        end_date=end_date,
                        compiled_groups=compiled_groups,
                        compiled_exclusions=compiled_exclusions,
                    )
                    snowball_screening_rows.append(row)
                    if include:
                        snowball_candidates_included.append(work)
                        work_id = str(work.get("id") or "")
                        if work_id:
                            raw_by_openalex_id[work_id] = work

                _write_csv(
                    output_dir / "snowball_candidates.csv",
                    [_openalex_row(work) for work in snowball_candidates_included],
                    SNOWBALL_COLUMNS,
                )
                _write_csv(
                    output_dir / "snowball_screening_log.csv",
                    snowball_screening_rows,
                    SCREENING_COLUMNS,
                )
            else:
                _write_csv(output_dir / "snowball_candidates.csv", [], SNOWBALL_COLUMNS)
                _write_csv(output_dir / "snowball_screening_log.csv", [], SCREENING_COLUMNS)

        if args.merge_snowball_into_core and snowball_candidates_included:
            snowball_models = [
                _to_research_work(work, source="OpenAlex Snowballing", track="core")
                for work in snowball_candidates_included
                if is_archival(work)
            ]
            merged_stats = _deduplicate_tracks([*final_core, *snowball_models], final_watchlist)
            final_core = merged_stats.core_works
            final_watchlist = merged_stats.watchlist_works

        metadata_rows = [*_map_research_works(final_core), *_map_research_works(final_watchlist)]
        _write_csv(output_dir / "metadata.csv", metadata_rows, METADATA_COLUMNS)
        _write_csv(output_dir / "screening_log.csv", screening_rows, SCREENING_COLUMNS)
        _export_double_screen_sample(
            metadata_rows,
            output_dir / "double_screen_sample.csv",
            fraction=args.double_screen_fraction,
            seed=args.double_screen_seed,
        )

        dedup_summary = _build_dedup_summary(
            raw_core_retrieved=len(core_raw),
            raw_preprint_retrieved=len(preprint_raw),
            included_topical_core_raw=len(included_core_raw),
            included_topical_preprint_raw=len(included_preprint_raw),
            dedup_stats=dedup_stats,
            resolution_result=resolution_result,
            final_core_count=len(final_core),
            final_preprint_watchlist_count=len(final_watchlist),
        )
        _write_csv(output_dir / "dedup_summary.csv", [dedup_summary], DEDUP_SUMMARY_COLUMNS)

        prisma_counts = _build_prisma_counts(
            raw_core_retrieved=len(core_raw),
            raw_preprint_retrieved=len(preprint_raw),
            included_topical_core_raw=len(included_core_raw),
            included_topical_preprint_raw=len(included_preprint_raw),
            core_after_dedup=len(dedup_stats.core_works),
            preprint_watchlist_after_dedup=len(dedup_stats.watchlist_works),
            preprints_resolved_to_existing_core=resolution_result.resolved_to_existing_core,
            preprints_resolved_to_new_archival=resolution_result.resolved_to_new_archival,
            final_export_core=len(final_core),
            final_export_preprint_watchlist=len(final_watchlist),
        )
        _write_prisma_counts(output_dir / "prisma_counts.csv", prisma_counts)

        _write_csv(output_dir / "preprint_resolution.csv", resolution_result.resolution_rows, PREPRINT_RESOLUTION_COLUMNS)

        screening_template_rows = _screening_template_rows(final_core)
        _write_csv(output_dir / "screening_title_abstract.csv", screening_template_rows, SCREENING_TEMPLATE_COLUMNS)
        _write_csv(output_dir / "screening_fulltext.csv", screening_template_rows, SCREENING_TEMPLATE_COLUMNS)

        quality_rows = _quality_appraisal_rows(final_core)
        _write_csv(output_dir / "quality_appraisal.csv", quality_rows, QUALITY_APPRAISAL_COLUMNS)

        if args.download_pdfs and final_core:
            pdf_candidates: list[dict[str, Any]] = []
            for work in final_core:
                if not work.openalex_id:
                    continue
                raw = raw_by_openalex_id.get(work.openalex_id)
                if raw is not None:
                    pdf_candidates.append(raw)

            pdf_candidates = pdf_candidates[: args.pdf_limit]
            async with PDFSectionExtractor(email=args.email, requests_per_second=args.pdf_rps) as pdf_extractor:
                pdf_results = await pdf_extractor.process_many(
                    pdf_candidates,
                    output_dir / "pdfs",
                    max_concurrency=args.pdf_concurrency,
                )

            _write_jsonl(output_dir / "pdf_framework_sections.jsonl", pdf_results)
            _write_csv(
                output_dir / "pdf_download_log.csv",
                [
                    {
                        "id": result.get("id", ""),
                        "title": result.get("title", ""),
                        "doi": normalize_doi(result.get("doi")) or "",
                        "status": result.get("status", ""),
                        "pdf_path": result.get("pdf_path", ""),
                    }
                    for result in pdf_results
                ],
                ["id", "title", "doi", "status", "pdf_path"],
            )


def main() -> None:
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
