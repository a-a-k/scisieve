from __future__ import annotations

import asyncio
import json
import math
import platform
import random
import re
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse

import httpx
import yaml

from api_clients import OpenAlexBudgetExceeded, OpenAlexClient, OpenCitationsClient
from extractor import PDFSectionExtractor, best_pdf_url_from_work
from models import (
    ResearchWork,
    infer_topic_labels,
    normalize_doi,
    reconstruct_abstract,
)
from protocol import (
    NEGATIVE_EXCLUSION_TERMS,
    build_screening_text,
    compile_terms_to_regex,
    compile_group_regex,
    compile_negative_regex,
    evaluate_protocol,
    flatten_term_groups,
    negative_exclusion_terms,
    screening_term_groups,
)
from snowballing import (
    backward_snowball,
    backward_snowball_opencitations,
    forward_snowball,
    forward_snowball_opencitations,
)

from . import __version__
from .common import (
    first_sentence,
    iso_utc_now,
    normalize_title,
    read_csv,
    read_jsonl,
    sha256_file,
    sha256_text,
    slugify,
    snippet_for_term,
    stable_json_dumps,
    unique_preserve_order,
    write_csv,
    write_json,
    write_jsonl,
    write_markdown,
)
from .config import ResolvedConfig


RETRIEVAL_YIELD_COLUMNS = [
    "pack_id",
    "raw_hits",
    "after_metadata_filter",
    "after_negative_exclusions",
    "after_dedup",
    "after_title_abstract_screen",
    "after_fulltext_screen",
    "included_core",
    "included_tertiary",
    "precision_proxy",
    "anchor_hits",
    "negative_sentinel_hits",
]
PACK_OVERLAP_COLUMNS = ["left_pack_id", "right_pack_id", "overlap_count"]
UNIQUE_HITS_COLUMNS = ["pack_id", "unique_hits"]
TOP_MISSING_ANCHORS_COLUMNS = ["pack_id", "anchor_id", "anchor_title", "tier", "expected_stage"]
NORMALIZED_COLUMNS = [
    "candidate_id",
    "source_name",
    "source_entity_id",
    "openalex_id",
    "doi",
    "title",
    "normalized_title",
    "abstract_text_reconstructed",
    "publication_year",
    "publication_date",
    "type",
    "type_crossref",
    "language",
    "cited_by_count",
    "indexed_in",
    "primary_source_name",
    "primary_source_type",
    "primary_source_is_core",
    "primary_source_is_in_doaj",
    "best_pdf_url",
    "has_abstract",
    "has_fulltext",
    "query_pack_ids",
    "query_terms_triggered",
    "retrieval_score_raw",
    "retrieval_score_norm",
    "retrieval_timestamp_utc",
    "snapshot_id",
    "record_hash",
    "stream",
]
METADATA_COLUMNS = [
    "record_id",
    "track",
    "archival_status",
    "source",
    "discovery_mode",
    "query_pack_ids",
    "query_terms_triggered",
    "anchor_match_ids",
    "negative_sentinel_match",
    "openalex_id",
    "doi",
    "title",
    "publication_year",
    "publication_date",
    "type",
    "primary_source_name",
    "cited_by_count",
    "best_pdf_url",
    "label_primary_dimension",
    "label_primary_value",
    "label_secondary_dimension",
    "label_secondary_value",
    "topic_labels_json",
    "machine_decision",
    "machine_confidence",
    "machine_reason",
    "matched_archival_id",
    "matched_archival_doi",
    "retrieval_score_norm",
]
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
PREPRINT_MATCH_CANDIDATES_COLUMNS = [
    "preprint_id",
    "preprint_title",
    "candidate_openalex_id",
    "candidate_doi",
    "candidate_title",
    "method",
    "similarity",
    "accepted",
]
SCREENING_TA_COLUMNS = [
    "record_id",
    "openalex_id",
    "doi",
    "title",
    "year",
    "query_pack_ids",
    "anchor_match_ids",
    "negative_sentinel_match",
    "machine_decision",
    "machine_confidence",
    "machine_reason",
    "reviewer1_decision",
    "reviewer1_reason",
    "reviewer2_decision",
    "reviewer2_reason",
    "resolved_decision",
    "resolved_reason",
    "decision_stage_timestamp",
    "notes",
]
SCREENING_FT_COLUMNS = SCREENING_TA_COLUMNS + [
    "pdf_local_path",
    "pdf_parse_status",
    "fulltext_sections_seen",
    "equations_detected",
    "figures_detected",
]
FULLTEXT_INVENTORY_COLUMNS = [
    "record_id",
    "openalex_id",
    "doi",
    "title",
    "track",
    "best_pdf_url",
    "download_attempted",
    "download_status",
    "pdf_local_path",
    "parse_status",
    "extraction_ready",
]
PDF_DOWNLOAD_COLUMNS = ["record_id", "best_pdf_url", "status", "pdf_local_path"]
PARSE_LOG_COLUMNS = ["record_id", "pdf_local_path", "parse_status", "sections_detected"]
EVIDENCE_COLUMNS = [
    "record_id",
    "label_primary_dimension",
    "label_primary_value",
    "label_secondary_dimension",
    "label_secondary_value",
    "topic_labels_json",
    "system_summary",
    "disturbance_summary",
    "method_summary",
    "workload_summary",
    "disturbance_type",
    "disturbance_locus",
    "disturbance_scope",
    "correlation_mode",
    "metrics_reported",
    "equations_reported",
    "validation_mode",
    "artifact_links",
    "key_findings",
    "threats_to_validity",
    "auto_confidence",
    "evidence_spans",
]
QUALITY_COLUMNS = [
    "record_id",
    "Q1_auto",
    "Q1_confidence",
    "Q1_evidence",
    "Q2_auto",
    "Q2_confidence",
    "Q2_evidence",
    "Q3_auto",
    "Q3_confidence",
    "Q3_evidence",
    "Q4_auto",
    "Q4_confidence",
    "Q4_evidence",
    "Q5_auto",
    "Q5_confidence",
    "Q5_evidence",
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
GRAY_CANDIDATES_COLUMNS = [
    "gray_id",
    "family_id",
    "organization",
    "domain",
    "url",
    "title",
    "artifact_type",
    "published_date",
    "last_reviewed_date",
    "retrieval_date",
    "content_hash",
    "archive_snapshot_url",
    "query_family",
    "technical_specificity_score",
    "provenance_score",
    "vendor_bias_risk",
    "aacods_authority",
    "aacods_accuracy",
    "aacods_coverage",
    "aacods_objectivity",
    "aacods_date",
    "aacods_significance",
    "auto_include_recommendation",
    "manual_final_decision",
]
GRAY_COUNTS_COLUMNS = ["family_id", "candidate_count", "include_recommended", "borderline", "reject"]
COVERAGE_COLUMNS = [
    "anchor_id",
    "polarity",
    "tier",
    "expected_stage",
    "indexed_in_primary_source",
    "retrieved",
    "retrieved_pack_ids",
    "lost_stage",
    "final_track",
    "matched_record_id",
    "diagnosis",
    "action_required",
]
NEGATIVE_SENTINEL_COLUMNS = [
    "anchor_id",
    "retrieved",
    "retrieved_pack_ids",
    "screen_ta_status",
    "final_track",
    "failed_precision_gate",
]
STAGE_LOSS_COLUMNS = ["lost_stage", "count"]
COVERAGE_FAILURE_COLUMNS = ["anchor_id", "tier", "diagnosis", "action_required"]
FALSE_NEGATIVE_COLUMNS = [
    "record_id",
    "title",
    "doi",
    "citation_count",
    "publication_year",
    "machine_decision",
    "score",
    "reason",
]
SNOWBALL_ROUNDS_COLUMNS = [
    "round_id",
    "seed_count",
    "backward_candidates",
    "forward_candidates",
    "after_dedup",
    "after_topical_filter",
    "after_ta_screen",
    "after_ft_screen",
    "new_core",
]
SNOWBALL_EDGES_COLUMNS = ["round_id", "seed_id", "candidate_id", "direction"]
SNOWBALL_CANDIDATES_COLUMNS = [
    "round_id",
    "candidate_id",
    "openalex_id",
    "title",
    "doi",
    "edge_count_from_seeds",
    "seed_diversity",
    "citation_direction",
    "anchor_proximity",
    "title_abstract_topical_score",
    "venue_signal",
    "recentness_weight",
    "priority_score",
    "decision",
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

HIGH_PRIORITY_CUES = [
    "formal analysis",
    "model checking",
    "probabilistic model checking",
    "fault injection",
    "controlled experiment",
    "evaluation framework",
    "reliability assessment",
    "continuity assessment",
    "risk assessment",
    "failure prediction",
    "fault diagnosis",
    "degradation analysis",
    "simulation",
    "simulator",
    "benchmark",
    "case study",
]
OUT_OF_SCOPE_PATTERNS = [
    "retail",
    "classroom",
    "curriculum",
    "agriculture",
    "flood model",
]
METRIC_TERMS = [
    "reliability",
    "robustness",
    "continuity",
    "availability",
    "recovery",
    "risk",
    "degradation",
    "failure",
]
METHOD_TERMS = [
    "formal analysis",
    "model checking",
    "simulation",
    "simulator",
    "experiment",
    "benchmark",
    "fault injection",
    "fault diagnosis",
    "failure prediction",
    "machine learning",
    "deep learning",
    "reinforcement learning",
    "analytical model",
]
TERTIARY_TERMS = [
    "survey",
    "review",
    "systematic literature review",
    "mapping study",
    "multivocal literature review",
]


@dataclass
class PipelineContext:
    config: ResolvedConfig
    query_pack_payload: dict[str, Any]
    gray_registry_payload: dict[str, Any]
    topic_profile_payload: dict[str, Any]
    anchor_rows: list[dict[str, Any]]
    compiled_groups: Mapping[str, Sequence[re.Pattern[str]]]
    compiled_exclusions: Sequence[re.Pattern[str]]


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
    match_candidate_rows: list[dict[str, Any]]
    resolved_to_existing_core: int
    resolved_to_new_archival: int


def _is_archival_work_type(work_type: str | None) -> bool:
    return str(work_type or "").strip().lower() in {"article", "proceedings-article"}


def _is_archival_work(work: Mapping[str, Any] | ResearchWork) -> bool:
    if isinstance(work, ResearchWork):
        return _is_archival_work_type(work.work_type)
    return _is_archival_work_type(str(work.get("type") or ""))


def _abstract_richness(work: ResearchWork) -> int:
    return len((work.abstract or "").split())


def _date_sort_key(work: ResearchWork) -> int:
    if work.publication_date:
        try:
            return datetime.fromisoformat(work.publication_date).date().toordinal()
        except ValueError:
            pass
    if isinstance(work.publication_year, int):
        try:
            return date(work.publication_year, 1, 1).toordinal()
        except ValueError:
            pass
    return 0


def _choose_preferred_work(left: ResearchWork, right: ResearchWork) -> ResearchWork:
    def ranking(work: ResearchWork) -> tuple[int, int, int, int]:
        if _is_archival_work(work):
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


def _canonical_keys(work: ResearchWork) -> list[tuple[str, str]]:
    keys: list[tuple[str, str]] = []
    if work.doi:
        keys.append(("doi", work.doi))
    title_key = normalize_title(work.title)
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


def _find_matching_work(work: ResearchWork, index: Mapping[tuple[str, str], ResearchWork]) -> ResearchWork | None:
    for key in _canonical_keys(work):
        existing = index.get(key)
        if existing is not None:
            return existing
    return None


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
        title_key = normalize_title(work.title)
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


def _git_hash() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception:
        return "unknown"
    return result.stdout.strip() or "unknown"


def _ensure_directories(ctx: PipelineContext) -> None:
    paths = ctx.config.paths
    for directory in (
        paths.run_root,
        paths.raw_dir,
        paths.raw_scholarly_dir,
        paths.raw_gray_dir,
        paths.normalized_dir,
        paths.reports_dir,
        paths.fulltext_dir,
        paths.release_dir,
        paths.tables_dir,
        paths.figures_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def _manifest_payload(ctx: PipelineContext) -> dict[str, Any]:
    cfg = ctx.config
    return {
        "run_id": cfg.run_id,
        "profile": cfg.profile_name,
        "topic_id": str(ctx.topic_profile_payload.get("topic_id") or cfg.app.topic),
        "tool_version": __version__,
        "git_commit_hash": _git_hash(),
        "run_timestamp_utc": cfg.run_timestamp_utc,
        "input_mode": cfg.profile.input_mode,
        "input_snapshot_id": cfg.profile.snapshot_id,
        "config_hash": sha256_file(cfg.config_path),
        "query_packs_hash": sha256_file(cfg.paths.query_packs_path),
        "topic_profile_hash": sha256_file(cfg.paths.topic_profile_path),
        "anchor_set_hash": sha256_file(cfg.paths.anchor_benchmark_path),
        "gray_registry_hash": sha256_file(cfg.paths.gray_registry_path),
        "python_version": sys.version.replace("\n", " "),
        "platform": platform.platform(),
        "contact_email": cfg.contact_email,
        "resolved_end_date": cfg.resolved_end_date,
        "non_production": cfg.profile.non_production,
    }


def _write_run_manifest(ctx: PipelineContext) -> None:
    _ensure_directories(ctx)
    write_json(ctx.config.paths.run_root / "run_manifest.json", _manifest_payload(ctx))


def _resume_state_path(ctx: PipelineContext) -> Path:
    return ctx.config.paths.run_root / "resume_state.json"


def _freeze_progress_path(ctx: PipelineContext) -> Path:
    return ctx.config.paths.run_root / "freeze_progress.json"


def _load_resume_state(ctx: PipelineContext) -> dict[str, Any]:
    path = _resume_state_path(ctx)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def _write_resume_state(
    ctx: PipelineContext,
    *,
    status: str,
    stage: str,
    detail: str = "",
    resume_after_utc: str = "",
) -> None:
    write_json(
        _resume_state_path(ctx),
        {
            "status": status,
            "stage": stage,
            "detail": detail,
            "resume_after_utc": resume_after_utc,
            "updated_at_utc": iso_utc_now(),
        },
    )


def _clear_resume_state(ctx: PipelineContext) -> None:
    path = _resume_state_path(ctx)
    if path.exists():
        path.unlink()


def _append_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _build_context(config: ResolvedConfig) -> PipelineContext:
    query_pack_payload = _load_yaml(config.paths.query_packs_path)
    gray_registry_payload = _load_yaml(config.paths.gray_registry_path)
    topic_profile_payload = _load_yaml(config.paths.topic_profile_path)
    anchor_rows = read_csv(config.paths.anchor_benchmark_path)
    term_groups = screening_term_groups(topic_profile_payload)
    exclusions = negative_exclusion_terms(topic_profile_payload)
    return PipelineContext(
        config=config,
        query_pack_payload=query_pack_payload,
        gray_registry_payload=gray_registry_payload,
        topic_profile_payload=topic_profile_payload,
        anchor_rows=anchor_rows,
        compiled_groups=compile_group_regex(term_groups),
        compiled_exclusions=compile_negative_regex(exclusions),
    )


def create_context(config: ResolvedConfig) -> PipelineContext:
    ctx = _build_context(config)
    _ensure_directories(ctx)
    _write_run_manifest(ctx)
    return ctx


def _pack_definitions(ctx: PipelineContext) -> list[dict[str, Any]]:
    packs = ctx.query_pack_payload.get("packs", [])
    return [pack for pack in packs if isinstance(pack, dict) and pack.get("enabled", True)]


def _topic_term_groups(ctx: PipelineContext) -> dict[str, list[str]]:
    return screening_term_groups(ctx.topic_profile_payload)


def _topic_group_terms(ctx: PipelineContext, *preferred_names: str) -> list[str]:
    groups = _topic_term_groups(ctx)
    collected: list[str] = []
    for name in preferred_names:
        collected.extend(groups.get(name, []))
    return unique_preserve_order(term for term in collected if term)


def _pack_negative_exclusions(ctx: PipelineContext) -> list[str]:
    base_terms = negative_exclusion_terms(ctx.topic_profile_payload)
    values = ctx.query_pack_payload.get("negative_exclusions", [])
    cleaned = [str(value).strip() for value in values if str(value).strip()]
    return unique_preserve_order([*base_terms, *cleaned]) or list(NEGATIVE_EXCLUSION_TERMS)


def _topic_classifier_terms(ctx: PipelineContext, key: str, fallback: Sequence[str]) -> list[str]:
    payload = ctx.topic_profile_payload.get("classifier", {})
    values = payload.get(key, [])
    if not isinstance(values, (list, tuple)):
        return list(fallback)
    cleaned = [str(value).strip() for value in values if str(value).strip()]
    return cleaned or list(fallback)


def _topic_classifier_title_rescue_rules(ctx: PipelineContext) -> list[dict[str, Any]]:
    payload = ctx.topic_profile_payload.get("classifier", {})
    values = payload.get("title_rescue_rules", [])
    return [value for value in values if isinstance(value, dict)]


def _topic_taxonomy_dimensions(ctx: PipelineContext) -> list[dict[str, Any]]:
    taxonomy = ctx.topic_profile_payload.get("taxonomy", {})
    values = taxonomy.get("dimensions", [])
    return [value for value in values if isinstance(value, dict)]


def _topic_metadata_config(ctx: PipelineContext) -> dict[str, Any]:
    payload = ctx.topic_profile_payload.get("metadata", {})
    return payload if isinstance(payload, dict) else {}


def _topic_label_assignments(ctx: PipelineContext, text: str) -> dict[str, str]:
    return infer_topic_labels(text, _topic_taxonomy_dimensions(ctx))


def _topic_label_fields(ctx: PipelineContext, labels: Mapping[str, str]) -> dict[str, str]:
    metadata_cfg = _topic_metadata_config(ctx)
    dimensions = _topic_taxonomy_dimensions(ctx)
    dimension_names = [str(item.get("name") or "") for item in dimensions if str(item.get("name") or "")]
    primary_dimension = str(metadata_cfg.get("primary_dimension") or (dimension_names[0] if dimension_names else ""))
    secondary_dimension = str(metadata_cfg.get("secondary_dimension") or (dimension_names[1] if len(dimension_names) > 1 else ""))
    fields = {
        "label_primary_dimension": primary_dimension,
        "label_primary_value": labels.get(primary_dimension, "") if primary_dimension else "",
        "label_secondary_dimension": secondary_dimension,
        "label_secondary_value": labels.get(secondary_dimension, "") if secondary_dimension else "",
        "topic_labels_json": stable_json_dumps(dict(sorted(labels.items()))),
    }
    compatibility_field_map = metadata_cfg.get("compatibility_field_map", metadata_cfg.get("legacy_field_map", {}))
    if isinstance(compatibility_field_map, dict):
        for field_name, dimension_name in compatibility_field_map.items():
            dimension_key = str(dimension_name or "").strip()
            fields[str(field_name)] = labels.get(dimension_key, "") if dimension_key else ""
    return fields


def _screening_text_from_row(row: Mapping[str, Any]) -> str:
    return " ".join(
        part
        for part in (
            str(row.get("title") or ""),
            str(row.get("abstract_text_reconstructed") or row.get("abstract") or ""),
        )
        if part
    ).strip()


def _topic_extraction_hints(ctx: PipelineContext, key: str, fallback: Sequence[str]) -> list[str]:
    payload = ctx.topic_profile_payload.get("extraction_hints", {})
    values = payload.get(key, [])
    if not isinstance(values, (list, tuple)):
        return list(fallback)
    cleaned = [str(value).strip() for value in values if str(value).strip()]
    return cleaned or list(fallback)


def _type_crossref_from_work(work: Mapping[str, Any]) -> str:
    primary_location = work.get("primary_location")
    if isinstance(primary_location, dict):
        source = primary_location.get("source")
        if isinstance(source, dict):
            host_org = source.get("host_organization_name")
            if isinstance(host_org, str):
                return host_org
    return ""


def _primary_source_metadata(work: Mapping[str, Any]) -> tuple[str, str, str, str]:
    primary_location = work.get("primary_location")
    if not isinstance(primary_location, dict):
        return "", "", "False", "False"
    source = primary_location.get("source")
    if not isinstance(source, dict):
        return "", "", "False", "False"
    name = str(source.get("display_name") or "")
    source_type = str(source.get("type") or "")
    is_core = str(bool(source.get("is_core")))
    is_in_doaj = str(bool(source.get("is_in_doaj")))
    return name, source_type, is_core, is_in_doaj


def _record_id_for_work(openalex_id: str, doi: str | None, title: str) -> str:
    base = openalex_id or doi or title
    return f"rec_{sha256_text(base)[:16]}"


def _candidate_lookup_key(work: Mapping[str, Any]) -> str:
    openalex_id = str(work.get("id") or "").strip()
    if openalex_id:
        return openalex_id
    doi = normalize_doi(str(work.get("doi") or ""))
    if doi:
        return f"doi:{doi}"
    return f"title:{normalize_title(str(work.get('title') or ''))}"


async def _iter_openalex_pages(
    openalex: OpenAlexClient,
    *,
    filter_expression: str,
    search_query: str,
    per_page: int,
    max_records: int | None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    cursor = "*"
    total = 0
    page_index = 0
    while True:
        params = openalex.auth_params(
            {
                "filter": filter_expression,
                "search": search_query,
                "per-page": per_page,
                "cursor": cursor,
            }
        )
        payload = await openalex._request_json("GET", "/works", params=params)  # noqa: SLF001
        page_results = payload.get("results", [])
        if not isinstance(page_results, list) or not page_results:
            break
        page_index += 1
        batch: list[dict[str, Any]] = []
        for result in page_results:
            if not isinstance(result, dict):
                continue
            total += 1
            batch.append(result)
            if max_records is not None and total >= max_records:
                break
        results.append(
            {
                "cursor": cursor,
                "page_index": page_index,
                "result_count": len(batch),
                "results": batch,
                "next_cursor": payload.get("meta", {}).get("next_cursor"),
            }
        )
        if max_records is not None and total >= max_records:
            break
        cursor = payload.get("meta", {}).get("next_cursor")
        if not cursor:
            break
    return results


def _raw_file_name(pack_id: str, stream: str) -> str:
    return f"{slugify(pack_id)}__{slugify(stream)}.jsonl"


def _raw_batch_summary_path(ctx: PipelineContext) -> Path:
    return ctx.config.paths.raw_scholarly_dir / "retrieval_batches.csv"


def _raw_pack_paths(ctx: PipelineContext) -> list[Path]:
    if not ctx.config.paths.raw_scholarly_dir.exists():
        return []
    return sorted(ctx.config.paths.raw_scholarly_dir.glob("*.jsonl"))


def _build_scholarly_filter(ctx: PipelineContext, types: Sequence[str], *, pack: Mapping[str, Any] | None = None) -> str:
    filter_payload = pack.get("filters", {}) if isinstance(pack, dict) else {}
    publication_year_from = filter_payload.get("publication_year_from")
    pack_types = filter_payload.get("types", [])
    allowed_types = [item for item in pack_types if isinstance(item, str)]
    stream_types = list(types)
    if allowed_types:
        stream_types = [item for item in stream_types if item in allowed_types]
    if not stream_types:
        return ""
    start_date = ctx.config.profile.scholarly.start_date
    if isinstance(publication_year_from, int):
        start_date = max(start_date, f"{publication_year_from:04d}-01-01")
    return OpenAlexClient.build_filter(
        types=stream_types,
        start_date=start_date,
        end_date=ctx.config.resolved_end_date,
        require_doi=False,
    )


def _basic_screening_text(work: Mapping[str, Any]) -> str:
    return build_screening_text(work)


@lru_cache(maxsize=1024)
def _compiled_term_patterns(terms: tuple[str, ...]) -> tuple[re.Pattern[str], ...]:
    return tuple(compile_terms_to_regex(list(terms)))


def _term_hits(text: str, terms: Sequence[str]) -> list[str]:
    normalized_text = re.sub(r"[\u2010-\u2015]", "-", text.lower())
    hits: list[str] = []
    normalized_terms = tuple(str(term).strip() for term in terms if str(term).strip())
    for term, pattern in zip(normalized_terms, _compiled_term_patterns(normalized_terms)):
        if pattern.search(normalized_text):
            hits.append(term)
    return unique_preserve_order(hits)


def _split_query_pack_ids(row: Mapping[str, Any]) -> set[str]:
    raw = str(row.get("query_pack_ids") or "")
    if not raw:
        return set()
    return {part.strip() for part in raw.split(";") if part.strip()}


def _title_rescue_match(
    ctx: PipelineContext,
    row: Mapping[str, Any],
    *,
    title: str,
    abstract: str,
    is_preprint: bool,
) -> dict[str, Any] | None:
    title_only = title.strip()
    if not title_only:
        return None
    query_pack_ids = _split_query_pack_ids(row)
    for rule in _topic_classifier_title_rescue_rules(ctx):
        if rule.get("only_when_abstract_missing", False) and abstract.strip():
            continue
        allowed_pack_ids = {
            str(value).strip()
            for value in rule.get("match_query_pack_ids", [])
            if str(value).strip()
        }
        if allowed_pack_ids and not (allowed_pack_ids & query_pack_ids):
            continue
        required_groups = rule.get("require_title_groups", {})
        if not isinstance(required_groups, dict):
            continue
        matched_terms: dict[str, list[str]] = {}
        all_groups_matched = True
        for group_name, terms in required_groups.items():
            if not isinstance(terms, (list, tuple)):
                all_groups_matched = False
                break
            hits = _term_hits(title_only, [str(term).strip() for term in terms if str(term).strip()])
            if not hits:
                all_groups_matched = False
                break
            matched_terms[str(group_name)] = hits
        if not all_groups_matched:
            continue
        decision_key = "preprint_decision" if is_preprint else "archival_decision"
        decision = str(rule.get(decision_key) or ("preprint_watchlist" if is_preprint else "include")).strip()
        if not decision:
            continue
        try:
            confidence = float(rule.get("confidence", 0.82))
        except (TypeError, ValueError):
            confidence = 0.82
        return {
            "rule_id": str(rule.get("id") or "title_rescue"),
            "decision": decision,
            "confidence": confidence,
            "matched_terms": matched_terms,
        }
    return None


def _candidate_spans(text: str, terms: Sequence[str], *, limit: int = 3) -> list[str]:
    snippets: list[str] = []
    for term in terms[:limit]:
        snippet = snippet_for_term(text, term)
        if snippet:
            snippets.append(f"{term}: {snippet}")
    return snippets


def _positive_anchor_rows(ctx: PipelineContext) -> list[dict[str, Any]]:
    return [row for row in ctx.anchor_rows if row.get("polarity") == "positive"]


def _negative_anchor_rows(ctx: PipelineContext) -> list[dict[str, Any]]:
    return [row for row in ctx.anchor_rows if row.get("polarity") == "negative"]


def _anchor_matches(ctx: PipelineContext, *, doi: str | None, title: str, polarity: str) -> list[str]:
    normalized_doi = normalize_doi(doi)
    normalized_title = normalize_title(title)
    matches: list[str] = []
    for row in ctx.anchor_rows:
        if row.get("polarity") != polarity:
            continue
        row_doi = normalize_doi(row.get("doi"))
        row_title = normalize_title(row.get("anchor_title") or row.get("title") or "")
        if normalized_doi and row_doi and normalized_doi == row_doi:
            matches.append(row["anchor_id"])
            continue
        if normalized_title and row_title and normalized_title == row_title:
            matches.append(row["anchor_id"])
    return unique_preserve_order(matches)


def _query_pack_hint_map(ctx: PipelineContext) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for row in _positive_anchor_rows(ctx):
        anchor_id = row.get("anchor_id", "")
        hint = row.get("query_pack_hint", "")
        if anchor_id and hint:
            mapping[anchor_id] = hint
    return mapping


def _format_query_pack_report(ctx: PipelineContext) -> str:
    lines: list[str] = []
    lines.append("QUERY_PACKS")
    for pack in _pack_definitions(ctx):
        lines.append(f"[{pack.get('id', '')}]")
        lines.append(str(pack.get("query", "")).strip())
        lines.append("")
    lines.append("NEGATIVE_EXCLUSIONS")
    for item in _pack_negative_exclusions(ctx):
        lines.append(item)
    lines.append("")
    lines.append(f"START_DATE: {ctx.config.profile.scholarly.start_date}")
    lines.append(f"END_DATE: {ctx.config.resolved_end_date}")
    lines.append(f"RUN_TIMESTAMP_UTC: {ctx.config.run_timestamp_utc}")
    return "\n".join(lines).rstrip() + "\n"


def _assert_profile_guards(ctx: PipelineContext) -> None:
    scholarly = ctx.config.profile.scholarly
    if not ctx.config.profile.non_production:
        if scholarly.max_records_per_pack is not None or scholarly.max_preprints_per_pack is not None:
            raise RuntimeError("Production profile cannot use scholarly retrieval caps.")


def _hash_row(payload: Mapping[str, Any]) -> str:
    return sha256_text(stable_json_dumps(payload))


def _metadata_filter_pass(work: Mapping[str, Any], ctx: PipelineContext) -> bool:
    title = str(work.get("title") or "").strip()
    work_type = str(work.get("type") or "")
    if not title or work_type not in {"article", "proceedings-article", "preprint"}:
        return False
    publication_date = str(work.get("publication_date") or "")
    if publication_date and publication_date > ctx.config.resolved_end_date:
        return False
    return True


def _load_raw_entries(ctx: PipelineContext) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for raw_path in _raw_pack_paths(ctx):
        entries.extend(read_jsonl(raw_path))
    return entries


async def stage_freeze_scholarly(ctx: PipelineContext) -> None:
    _ensure_directories(ctx)
    _write_run_manifest(ctx)
    if not _pack_definitions(ctx):
        raise RuntimeError("No enabled query packs configured.")

    retrieval_batch_columns = ["pack_id", "stream", "page_index", "cursor", "result_count", "search_query", "filter_expression"]
    batch_rows = read_csv(_raw_batch_summary_path(ctx)) if _raw_batch_summary_path(ctx).exists() else []
    progress_path = _freeze_progress_path(ctx)
    if progress_path.exists():
        progress_payload = json.loads(progress_path.read_text(encoding="utf-8"))
        task_rows = [row for row in progress_payload.get("tasks", []) if isinstance(row, dict)]
    else:
        completed_pairs = {
            (str(row.get("pack_id") or ""), str(row.get("stream") or ""))
            for row in batch_rows
            if str(row.get("pack_id") or "") and str(row.get("stream") or "")
        }
        task_rows: list[dict[str, Any]] = []
        for pack in _pack_definitions(ctx):
            pack_id = str(pack.get("id") or "")
            pack_query = str(pack.get("query") or "").strip()
            scholarly_limit = ctx.config.profile.scholarly.max_records_per_pack
            preprint_limit = ctx.config.profile.scholarly.max_preprints_per_pack
            streams = [
                ("scholarly", ["article", "proceedings-article"], scholarly_limit),
                ("preprint", ["preprint"], preprint_limit),
            ]
            for stream, types, limit in streams:
                filter_expression = _build_scholarly_filter(ctx, types, pack=pack)
                if not filter_expression:
                    continue
                task_rows.append(
                    {
                        "pack_id": pack_id,
                        "stream": stream,
                        "search_query": pack_query,
                        "filter_expression": filter_expression,
                        "limit": limit,
                        "next_cursor": "*" if (pack_id, stream) not in completed_pairs else "",
                        "page_index": 0,
                        "result_count": 0,
                        "completed": (pack_id, stream) in completed_pairs,
                    }
                )

    def persist_progress() -> None:
        write_json(
            progress_path,
            {
                "status": "in_progress",
                "updated_at_utc": iso_utc_now(),
                "tasks": task_rows,
            },
        )
        write_csv(_raw_batch_summary_path(ctx), batch_rows, retrieval_batch_columns)

    persist_progress()
    try:
        async with OpenAlexClient(
            email=ctx.config.contact_email,
            requests_per_second=7.0,
            api_key=ctx.config.openalex_api_key,
        ) as openalex:
            for task in task_rows:
                if task.get("completed"):
                    continue
                pack_id = str(task.get("pack_id") or "")
                stream = str(task.get("stream") or "")
                search_query = str(task.get("search_query") or "")
                filter_expression = str(task.get("filter_expression") or "")
                raw_path = ctx.config.paths.raw_scholarly_dir / _raw_file_name(pack_id, stream)
                cursor = str(task.get("next_cursor") or "*")
                page_index = int(task.get("page_index") or 0)
                result_count = int(task.get("result_count") or 0)
                limit = task.get("limit")

                while cursor:
                    params = openalex.auth_params(
                        {
                            "filter": filter_expression,
                            "search": search_query,
                            "per-page": ctx.config.profile.scholarly.per_page,
                            "cursor": cursor,
                        }
                    )
                    payload = await openalex._request_json("GET", "/works", params=params)  # noqa: SLF001
                    page_results = payload.get("results", [])
                    if not isinstance(page_results, list) or not page_results:
                        task["completed"] = True
                        task["next_cursor"] = ""
                        persist_progress()
                        break

                    remaining = None if limit is None else max(0, int(limit) - result_count)
                    batch = [row for row in page_results if isinstance(row, dict)]
                    if remaining is not None:
                        batch = batch[:remaining]

                    page_index += 1
                    raw_rows = [
                        {
                            "query_pack_id": pack_id,
                            "stream": stream,
                            "source_name": "OpenAlex",
                            "retrieved_at_utc": ctx.config.run_timestamp_utc,
                            "snapshot_id": ctx.config.profile.snapshot_id,
                            "filter_expression": filter_expression,
                            "search_query": search_query,
                            "page_cursor": cursor,
                            "page_index": page_index,
                            "retrieval_order": result_count + index + 1,
                            "work": result,
                        }
                        for index, result in enumerate(batch)
                    ]
                    _append_jsonl(raw_path, raw_rows)
                    batch_rows.append(
                        {
                            "pack_id": pack_id,
                            "stream": stream,
                            "page_index": page_index,
                            "cursor": cursor,
                            "result_count": len(batch),
                            "search_query": search_query,
                            "filter_expression": filter_expression,
                        }
                    )
                    result_count += len(batch)
                    next_cursor = payload.get("meta", {}).get("next_cursor")
                    task["page_index"] = page_index
                    task["result_count"] = result_count
                    task["next_cursor"] = "" if not next_cursor else str(next_cursor)
                    if (limit is not None and result_count >= int(limit)) or not next_cursor or not batch:
                        task["completed"] = True
                        task["next_cursor"] = ""
                    persist_progress()
                    if task.get("completed"):
                        break
                    cursor = str(task.get("next_cursor") or "")
    except OpenAlexBudgetExceeded:
        write_json(
            progress_path,
            {
                "status": "paused_budget",
                "updated_at_utc": iso_utc_now(),
                "tasks": task_rows,
            },
        )
        write_csv(_raw_batch_summary_path(ctx), batch_rows, retrieval_batch_columns)
        raise

    write_csv(_raw_batch_summary_path(ctx), batch_rows, retrieval_batch_columns)
    if progress_path.exists():
        progress_path.unlink()
    write_markdown(ctx.config.paths.run_root / "search_strings.txt", _format_query_pack_report(ctx))


def stage_retrieve_scholarly(ctx: PipelineContext) -> None:
    raw_entries = _load_raw_entries(ctx)
    if not raw_entries:
        raise RuntimeError("Raw scholarly freeze is missing. Run `scisieve freeze scholarly` first.")

    grouped: dict[str, dict[str, Any]] = {}
    for entry in raw_entries:
        work = entry.get("work", {})
        if not isinstance(work, dict):
            continue
        key = _candidate_lookup_key(work)
        current = grouped.setdefault(
            key,
            {
                "openalex_id": str(work.get("id") or ""),
                "doi": normalize_doi(str(work.get("doi") or "")) or "",
                "title": str(work.get("title") or ""),
                "type": str(work.get("type") or ""),
                "query_pack_ids": [],
                "streams": [],
                "retrieval_orders": [],
                "retrieved_at_utc": [],
                "raw_payloads": [],
            },
        )
        current["query_pack_ids"].append(str(entry.get("query_pack_id") or ""))
        current["streams"].append(str(entry.get("stream") or ""))
        current["retrieval_orders"].append(int(entry.get("retrieval_order") or 0))
        current["retrieved_at_utc"].append(str(entry.get("retrieved_at_utc") or ""))
        current["raw_payloads"].append(work)
        if len(json.dumps(work)) > len(json.dumps(current["raw_payloads"][0])):
            current["raw_payloads"][0] = work

    union_rows: list[dict[str, Any]] = []
    for grouped_row in grouped.values():
        raw_payload = grouped_row["raw_payloads"][0]
        union_rows.append(
            {
                "key": _candidate_lookup_key(raw_payload),
                "openalex_id": grouped_row["openalex_id"],
                "doi": grouped_row["doi"],
                "title": grouped_row["title"],
                "type": grouped_row["type"],
                "query_pack_ids": unique_preserve_order(grouped_row["query_pack_ids"]),
                "streams": unique_preserve_order(grouped_row["streams"]),
                "retrieval_order_best": min((value for value in grouped_row["retrieval_orders"] if value), default=0),
                "retrieved_at_utc": min(grouped_row["retrieved_at_utc"]) if grouped_row["retrieved_at_utc"] else "",
                "raw_work": raw_payload,
            }
        )

    write_jsonl(ctx.config.paths.normalized_dir / "retrieved_union.jsonl", union_rows)
    write_csv(
        ctx.config.paths.run_root / "retrieved_union.csv",
        [
            {
                "openalex_id": row["openalex_id"],
                "doi": row["doi"],
                "title": row["title"],
                "type": row["type"],
                "query_pack_ids": ";".join(row["query_pack_ids"]),
                "streams": ";".join(row["streams"]),
                "retrieval_order_best": row["retrieval_order_best"],
            }
            for row in union_rows
        ],
        ["openalex_id", "doi", "title", "type", "query_pack_ids", "streams", "retrieval_order_best"],
    )


def stage_normalize(ctx: PipelineContext) -> None:
    union_rows = read_jsonl(ctx.config.paths.normalized_dir / "retrieved_union.jsonl")
    if not union_rows:
        raise RuntimeError("Retrieved union is missing. Run `scisieve retrieve scholarly` first.")

    normalized_rows: list[dict[str, Any]] = []
    for item in union_rows:
        raw_work = item.get("raw_work", {})
        if not isinstance(raw_work, dict):
            continue
        text = _basic_screening_text(raw_work)
        topic_terms = flatten_term_groups(_topic_term_groups(ctx))
        query_terms_triggered = unique_preserve_order(
            _term_hits(text, topic_terms)
        )
        primary_source_name, primary_source_type, primary_source_is_core, primary_source_is_in_doaj = _primary_source_metadata(
            raw_work
        )
        best_pdf_url = raw_work.get("best_oa_location", {}).get("pdf_url") if isinstance(raw_work.get("best_oa_location"), dict) else ""
        if not best_pdf_url:
            primary_location = raw_work.get("primary_location")
            if isinstance(primary_location, dict):
                best_pdf_url = str(primary_location.get("pdf_url") or "")
        retrieval_rank = int(item.get("retrieval_order_best") or 0)
        score_raw = 1.0 / retrieval_rank if retrieval_rank else 0.0
        normalized_rows.append(
            {
                "candidate_id": _record_id_for_work(str(raw_work.get("id") or ""), raw_work.get("doi"), str(raw_work.get("title") or "")),
                "source_name": "OpenAlex",
                "source_entity_id": str(raw_work.get("id") or ""),
                "openalex_id": str(raw_work.get("id") or ""),
                "doi": normalize_doi(str(raw_work.get("doi") or "")) or "",
                "title": str(raw_work.get("title") or ""),
                "normalized_title": normalize_title(str(raw_work.get("title") or "")),
                "abstract_text_reconstructed": build_screening_text({"title": "", "abstract_inverted_index": raw_work.get("abstract_inverted_index")}),
                "publication_year": raw_work.get("publication_year") or "",
                "publication_date": raw_work.get("publication_date") or "",
                "type": raw_work.get("type") or "",
                "type_crossref": _type_crossref_from_work(raw_work),
                "language": raw_work.get("language") or "",
                "cited_by_count": raw_work.get("cited_by_count") or 0,
                "indexed_in": "OpenAlex",
                "primary_source_name": primary_source_name,
                "primary_source_type": primary_source_type,
                "primary_source_is_core": primary_source_is_core,
                "primary_source_is_in_doaj": primary_source_is_in_doaj,
                "best_pdf_url": best_pdf_url or "",
                "has_abstract": str(bool(raw_work.get("abstract_inverted_index"))),
                "has_fulltext": "False",
                "query_pack_ids": ";".join(item.get("query_pack_ids", [])),
                "query_terms_triggered": ";".join(query_terms_triggered),
                "retrieval_score_raw": f"{score_raw:.6f}",
                "retrieval_score_norm": f"{min(score_raw * 10.0, 1.0):.6f}",
                "retrieval_timestamp_utc": item.get("retrieved_at_utc", ""),
                "snapshot_id": ctx.config.profile.snapshot_id,
                "record_hash": _hash_row(raw_work),
                "stream": ";".join(item.get("streams", [])),
                "raw_work": raw_work,
            }
        )

    write_jsonl(ctx.config.paths.normalized_dir / "normalized_candidates.jsonl", normalized_rows)
    write_csv(ctx.config.paths.run_root / "normalized_candidates.csv", normalized_rows, NORMALIZED_COLUMNS)


def _normalized_rows(ctx: PipelineContext) -> list[dict[str, Any]]:
    return read_jsonl(ctx.config.paths.normalized_dir / "normalized_candidates.jsonl")


def _row_to_research_work(ctx: PipelineContext, row: Mapping[str, Any], *, source: str, track: str) -> ResearchWork:
    screening_text = _screening_text_from_row(row)
    topic_labels = _topic_label_assignments(ctx, screening_text)
    label_fields = _topic_label_fields(ctx, topic_labels)
    return ResearchWork(
        source=source,
        openalex_id=str(row.get("openalex_id") or "") or None,
        title=str(row.get("title") or ""),
        doi=str(row.get("doi") or "") or None,
        type=str(row.get("type") or ""),
        publication_date=str(row.get("publication_date") or "") or None,
        publication_year=int(row.get("publication_year") or 0) or None,
        track=track,
        archival_status="archival" if str(row.get("type") or "") != "preprint" else "preprint_watchlist",
        is_in_core_corpus=track == "core",
        is_watchlist_candidate=track == "preprint_watchlist",
        watchlist_tag="Candidate for Preprint Watchlist (trends-only)" if track == "preprint_watchlist" else None,
        replaced_by_doi=None,
        matched_archival_id=str(row.get("matched_archival_id") or "") or None,
        matched_archival_doi=str(row.get("matched_archival_doi") or "") or None,
        label_primary_dimension=label_fields["label_primary_dimension"] or None,
        label_primary_value=label_fields["label_primary_value"] or None,
        label_secondary_dimension=label_fields["label_secondary_dimension"] or None,
        label_secondary_value=label_fields["label_secondary_value"] or None,
        topic_labels_json=label_fields.get("topic_labels_json") or None,
        cited_by_count=int(row.get("cited_by_count") or 0) or None,
        best_pdf_url=str(row.get("best_pdf_url") or "") or None,
        abstract=str(row.get("abstract_text_reconstructed") or row.get("abstract") or "") or None,
    )


def _raw_work_to_research_work(
    ctx: PipelineContext,
    work: Mapping[str, Any],
    *,
    source: str,
    track: str,
    resolved_from_preprint: bool = False,
) -> ResearchWork:
    screening_text = build_screening_text(work)
    topic_labels = _topic_label_assignments(ctx, screening_text)
    label_fields = _topic_label_fields(ctx, topic_labels)
    return ResearchWork(
        source=source,
        openalex_id=str(work.get("id") or "") or None,
        title=str(work.get("title") or ""),
        doi=normalize_doi(work.get("doi")),
        type=str(work.get("type") or ""),
        publication_date=str(work.get("publication_date") or "") or None,
        publication_year=int(work.get("publication_year") or 0) or None,
        track=track,
        archival_status="archival" if _is_archival_work(work) else "preprint_watchlist",
        is_in_core_corpus=track == "core",
        is_watchlist_candidate=track == "preprint_watchlist",
        watchlist_tag="Candidate for Preprint Watchlist (trends-only)" if track == "preprint_watchlist" else None,
        replaced_by_doi=None,
        matched_archival_id=(str(work.get("id") or "") or None) if resolved_from_preprint else None,
        matched_archival_doi=normalize_doi(work.get("doi")) if resolved_from_preprint else None,
        resolved_from_preprint=resolved_from_preprint,
        label_primary_dimension=label_fields["label_primary_dimension"] or None,
        label_primary_value=label_fields["label_primary_value"] or None,
        label_secondary_dimension=label_fields["label_secondary_dimension"] or None,
        label_secondary_value=label_fields["label_secondary_value"] or None,
        topic_labels_json=label_fields.get("topic_labels_json") or None,
        cited_by_count=int(work.get("cited_by_count") or 0) or None,
        best_pdf_url=best_pdf_url_from_work(work),
        abstract=reconstruct_abstract(work.get("abstract_inverted_index")),
    )


async def _search_archival_match_by_title(
    ctx: PipelineContext,
    openalex: OpenAlexClient,
    title: str,
    *,
    threshold: float,
) -> tuple[dict[str, Any] | None, float]:
    title_norm = normalize_title(title)
    if not title_norm:
        return None, 0.0

    filter_expression = OpenAlexClient.build_filter(
        types=["article", "proceedings-article"],
        start_date=ctx.config.profile.scholarly.start_date,
        end_date=ctx.config.resolved_end_date,
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
        if not _is_archival_work(candidate):
            continue
        matched, _reason = evaluate_protocol(
            build_screening_text(candidate),
            compiled_groups=ctx.compiled_groups,
            compiled_exclusions=ctx.compiled_exclusions,
        )
        if not matched:
            continue
        candidate_norm = normalize_title(str(candidate.get("title") or ""))
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
    ctx: PipelineContext,
    openalex: OpenAlexClient,
    *,
    core_works: Sequence[ResearchWork],
    watchlist_works: Sequence[ResearchWork],
) -> ResolutionResult:
    core_current = list(core_works)
    core_index = _build_canonical_index(core_current)
    unresolved_watchlist: list[ResearchWork] = []
    resolution_rows: list[dict[str, Any]] = []
    match_candidate_rows: list[dict[str, Any]] = []
    resolved_to_existing_core = 0
    resolved_to_new_archival = 0

    for preprint in watchlist_works:
        matched_core: ResearchWork | None = None
        method = ""
        action = ""
        similarity = 0.0

        normalized_title = normalize_title(preprint.title)
        if normalized_title:
            matched_core = core_index.get(("title", normalized_title))
        if matched_core is not None:
            method = "exact_title"
            action = "matched_existing_core"
            similarity = 1.0
            resolved_to_existing_core += 1
            match_candidate_rows.append(
                {
                    "preprint_id": preprint.openalex_id or "",
                    "preprint_title": preprint.title,
                    "candidate_openalex_id": matched_core.openalex_id or "",
                    "candidate_doi": matched_core.doi or "",
                    "candidate_title": matched_core.title,
                    "method": method,
                    "similarity": "1.0000",
                    "accepted": "true",
                }
            )
        elif ctx.config.profile.scholarly.resolve_preprints and preprint.title.strip():
            matched_work, similarity = await _search_archival_match_by_title(
                ctx,
                openalex,
                preprint.title,
                threshold=ctx.config.profile.scholarly.resolve_preprints_threshold,
            )
            if matched_work is not None:
                match_candidate_rows.append(
                    {
                        "preprint_id": preprint.openalex_id or "",
                        "preprint_title": preprint.title,
                        "candidate_openalex_id": str(matched_work.get("id") or ""),
                        "candidate_doi": normalize_doi(matched_work.get("doi")) or "",
                        "candidate_title": str(matched_work.get("title") or ""),
                        "method": "title_search_openalex",
                        "similarity": f"{similarity:.4f}",
                        "accepted": "true",
                    }
                )
                matched_candidate = _raw_work_to_research_work(
                    ctx,
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

    return ResolutionResult(
        core_works=core_current,
        watchlist_works=unresolved_watchlist,
        resolution_rows=resolution_rows,
        match_candidate_rows=match_candidate_rows,
        resolved_to_existing_core=resolved_to_existing_core,
        resolved_to_new_archival=resolved_to_new_archival,
    )


def _topic_classifier(ctx: PipelineContext, row: Mapping[str, Any]) -> dict[str, Any]:
    title = str(row.get("title") or "")
    abstract = str(row.get("abstract_text_reconstructed") or row.get("abstract") or "")
    text = " ".join(part for part in (title, abstract) if part).strip()
    context_terms = _topic_group_terms(ctx, "context", "system", "platform", "deployment")
    method_group_terms = _topic_group_terms(ctx, "method", "model_paradigm", "approach")
    metric_group_terms = _topic_group_terms(ctx, "phenomenon", "construct", "outcome", "objective")
    matched_protocol, protocol_reason = evaluate_protocol(
        text,
        compiled_groups=ctx.compiled_groups,
        compiled_exclusions=ctx.compiled_exclusions,
    )
    context_hits = _term_hits(text, context_terms)
    method_hits = _term_hits(
        text,
        unique_preserve_order([*_topic_classifier_terms(ctx, "method_terms", METHOD_TERMS), *method_group_terms]),
    )
    metric_hits = _term_hits(
        text,
        unique_preserve_order([*_topic_classifier_terms(ctx, "metric_terms", METRIC_TERMS), *metric_group_terms]),
    )
    tertiary_hits = _term_hits(text, _topic_classifier_terms(ctx, "tertiary_terms", TERTIARY_TERMS))
    high_priority_hits = _term_hits(text, _topic_classifier_terms(ctx, "high_priority_cues", HIGH_PRIORITY_CUES))
    out_hits = _term_hits(text, unique_preserve_order([*_topic_classifier_terms(ctx, "out_of_scope_patterns", OUT_OF_SCOPE_PATTERNS), *_pack_negative_exclusions(ctx)]))
    negative_anchor_hits = _anchor_matches(ctx, doi=row.get("doi"), title=title, polarity="negative")
    positive_anchor_hits = _anchor_matches(ctx, doi=row.get("doi"), title=title, polarity="positive")
    positive_anchor_expected_stages = {
        str(anchor.get("expected_stage") or "")
        for anchor in ctx.anchor_rows
        if anchor.get("anchor_id") in positive_anchor_hits
    }

    is_preprint = str(row.get("type") or "") == "preprint" or str(row.get("archival_status") or "") == "preprint_watchlist"
    title_rescue = _title_rescue_match(
        ctx,
        row,
        title=title,
        abstract=abstract,
        is_preprint=is_preprint,
    )
    decision = "exclude"
    confidence = 0.55
    reasons: list[str] = []

    if negative_anchor_hits:
        decision = "exclude"
        confidence = 0.99
        reasons.append(f"negative_sentinel={','.join(negative_anchor_hits)}")
    elif out_hits and not high_priority_hits:
        decision = "exclude"
        confidence = 0.92
        reasons.append(f"out_of_scope={','.join(out_hits[:4])}")
    elif tertiary_hits:
        decision = "tertiary_background"
        confidence = 0.9 if high_priority_hits else 0.86
        reasons.append(f"tertiary={','.join(tertiary_hits[:4])}")
    elif title_rescue is not None:
        decision = str(title_rescue["decision"])
        confidence = float(title_rescue["confidence"])
        matched_terms = title_rescue["matched_terms"]
        context_hits = unique_preserve_order([*context_hits, *matched_terms.get("context", [])])
        method_hits = unique_preserve_order([*method_hits, *matched_terms.get("method", [])])
        metric_hits = unique_preserve_order([*metric_hits, *matched_terms.get("metric", [])])
        reasons.append(f"title_rescue_rule={title_rescue['rule_id']}")
    elif matched_protocol and context_hits and method_hits and metric_hits:
        decision = "preprint_watchlist" if is_preprint else "include"
        confidence = 0.93 if high_priority_hits else 0.86
        reasons.append("protocol_match")
    elif matched_protocol and context_hits and method_hits:
        decision = "needs_fulltext"
        confidence = 0.64
        reasons.append("missing_metric_signal")
    elif context_hits and (method_hits or metric_hits):
        decision = "borderline"
        confidence = 0.51
        reasons.append("partial_scope_match")
    else:
        decision = "exclude"
        confidence = 0.75 if protocol_reason else 0.6
        reasons.append(protocol_reason or "insufficient_scope")

    if positive_anchor_hits:
        reasons.append(f"anchor_match={','.join(positive_anchor_hits)}")
        if "discovery_only_or_tertiary" in positive_anchor_expected_stages and decision != "include":
            decision = "tertiary_background"
            confidence = max(confidence, 0.82)
            reasons.append("anchor_expected_stage=tertiary")
        elif decision in {"exclude", "borderline"}:
            decision = "needs_fulltext"
            confidence = max(confidence, 0.7)

    reasons.append(f"context={','.join(context_hits[:4]) or 'none'}")
    reasons.append(f"method={','.join(method_hits[:4]) or 'none'}")
    reasons.append(f"metric={','.join(metric_hits[:4]) or 'none'}")

    reason_text = "; ".join(reasons)
    evidence_terms = unique_preserve_order([*context_hits, *method_hits, *metric_hits, *high_priority_hits])
    evidence = " | ".join(_candidate_spans(text, evidence_terms, limit=3))
    if evidence:
        reason_text = f"{reason_text}; evidence={evidence}"

    return {
        "machine_decision": decision,
        "machine_confidence": f"{confidence:.2f}",
        "machine_reason": reason_text,
        "anchor_match_ids": ";".join(positive_anchor_hits),
        "negative_sentinel_match": ";".join(negative_anchor_hits),
        "topical_score": len(context_hits) + len(method_hits) + len(metric_hits) + len(high_priority_hits),
    }


def _screening_row(record_id: str, row: Mapping[str, Any], classifier: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "record_id": record_id,
        "openalex_id": row.get("openalex_id", ""),
        "doi": row.get("doi", ""),
        "title": row.get("title", ""),
        "year": row.get("publication_year", ""),
        "query_pack_ids": row.get("query_pack_ids", ""),
        "anchor_match_ids": classifier.get("anchor_match_ids", ""),
        "negative_sentinel_match": classifier.get("negative_sentinel_match", ""),
        "machine_decision": classifier.get("machine_decision", ""),
        "machine_confidence": classifier.get("machine_confidence", ""),
        "machine_reason": classifier.get("machine_reason", ""),
        "reviewer1_decision": "",
        "reviewer1_reason": "",
        "reviewer2_decision": "",
        "reviewer2_reason": "",
        "resolved_decision": classifier.get("machine_decision", ""),
        "resolved_reason": "",
        "decision_stage_timestamp": iso_utc_now(),
        "notes": "",
    }


def _split_multi_value(value: Any) -> list[str]:
    return [item for item in str(value or "").split(";") if item]


def _merge_multi_value(*values: Any) -> str:
    merged: list[str] = []
    for value in values:
        merged.extend(_split_multi_value(value))
    return ";".join(unique_preserve_order(merged))


def _candidate_row_key(row: Mapping[str, Any]) -> str:
    openalex_id = str(row.get("openalex_id") or "").strip()
    if openalex_id:
        return f"openalex:{openalex_id}"
    doi = normalize_doi(row.get("doi"))
    if doi:
        return f"doi:{doi}"
    title = normalize_title(str(row.get("title") or ""))
    return f"title:{title}"


def _track_rank(track: str) -> int:
    return {
        "scholarly_core": 3,
        "scholarly_tertiary": 2,
        "preprint_watchlist": 1,
    }.get(track, 0)


def _metadata_canonical_keys(row: Mapping[str, Any]) -> list[tuple[str, str]]:
    keys: list[tuple[str, str]] = []
    openalex_id = str(row.get("openalex_id") or "").strip()
    if openalex_id:
        keys.append(("openalex", openalex_id))
    doi = normalize_doi(row.get("doi"))
    if doi:
        keys.append(("doi", doi))
    title = normalize_title(str(row.get("title") or ""))
    if title:
        keys.append(("title", title))
    return keys


def _metadata_rows_overlap(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return bool(set(_metadata_canonical_keys(left)) & set(_metadata_canonical_keys(right)))


def _metadata_row_rank(row: Mapping[str, Any]) -> tuple[int, int, int, int, str]:
    work_type = str(row.get("type") or "")
    source = str(row.get("source") or "")
    archival_rank = 0
    if _is_archival_work_type(work_type):
        archival_rank = 1 if "Title Resolution" in source else 2
    publication_date = str(row.get("publication_date") or "")
    publication_year = str(row.get("publication_year") or "")
    date_rank = 0
    if publication_date:
        try:
            date_rank = datetime.fromisoformat(publication_date).date().toordinal()
        except ValueError:
            date_rank = 0
    elif publication_year.isdigit():
        date_rank = date(int(publication_year), 1, 1).toordinal()
    return (
        _track_rank(str(row.get("track") or "")),
        archival_rank,
        int(row.get("cited_by_count", 0) or 0),
        date_rank,
        str(row.get("openalex_id") or ""),
    )


def _merge_metadata_pair(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    left_row = dict(left)
    right_row = dict(right)
    stronger = left_row if _metadata_row_rank(left_row) >= _metadata_row_rank(right_row) else right_row
    weaker = right_row if stronger is left_row else left_row

    stronger["source"] = _merge_multi_value(left_row.get("source"), right_row.get("source"))
    stronger["discovery_mode"] = _merge_multi_value(left_row.get("discovery_mode"), right_row.get("discovery_mode"))
    stronger["query_pack_ids"] = _merge_multi_value(left_row.get("query_pack_ids"), right_row.get("query_pack_ids"))
    stronger["query_terms_triggered"] = _merge_multi_value(
        left_row.get("query_terms_triggered"),
        right_row.get("query_terms_triggered"),
    )
    stronger["anchor_match_ids"] = _merge_multi_value(left_row.get("anchor_match_ids"), right_row.get("anchor_match_ids"))
    stronger["negative_sentinel_match"] = _merge_multi_value(
        left_row.get("negative_sentinel_match"),
        right_row.get("negative_sentinel_match"),
    )
    stronger["matched_archival_id"] = stronger.get("matched_archival_id") or weaker.get("matched_archival_id") or ""
    stronger["matched_archival_doi"] = stronger.get("matched_archival_doi") or weaker.get("matched_archival_doi") or ""
    stronger["machine_reason"] = " | ".join(
        unique_preserve_order(
            value for value in [left_row.get("machine_reason", ""), right_row.get("machine_reason", "")] if value
        )
    )
    stronger_confidence = max(
        float(left_row.get("machine_confidence", 0) or 0),
        float(right_row.get("machine_confidence", 0) or 0),
    )
    stronger["machine_confidence"] = f"{stronger_confidence:.2f}" if stronger_confidence else ""
    stronger["retrieval_score_norm"] = stronger.get("retrieval_score_norm") or weaker.get("retrieval_score_norm") or ""
    stronger["best_pdf_url"] = stronger.get("best_pdf_url") or weaker.get("best_pdf_url") or ""
    stronger["primary_source_name"] = stronger.get("primary_source_name") or weaker.get("primary_source_name") or ""
    stronger["record_id"] = stronger.get("record_id") or weaker.get("record_id") or ""
    stronger["archival_status"] = stronger.get("archival_status") or weaker.get("archival_status") or ""
    stronger["label_primary_dimension"] = stronger.get("label_primary_dimension") or weaker.get("label_primary_dimension") or ""
    stronger["label_primary_value"] = stronger.get("label_primary_value") or weaker.get("label_primary_value") or ""
    stronger["label_secondary_dimension"] = stronger.get("label_secondary_dimension") or weaker.get("label_secondary_dimension") or ""
    stronger["label_secondary_value"] = stronger.get("label_secondary_value") or weaker.get("label_secondary_value") or ""
    stronger["topic_labels_json"] = stronger.get("topic_labels_json") or weaker.get("topic_labels_json") or ""
    return stronger


def _deduplicate_metadata_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    for row in rows:
        candidate = dict(row)
        merged_any = True
        while merged_any:
            merged_any = False
            next_rows: list[dict[str, Any]] = []
            for existing in deduped:
                if _metadata_rows_overlap(existing, candidate):
                    candidate = _merge_metadata_pair(existing, candidate)
                    merged_any = True
                else:
                    next_rows.append(existing)
            deduped = next_rows
        deduped.append(candidate)
    deduped.sort(key=lambda row: (str(row.get("track") or ""), str(row.get("title") or ""), str(row.get("openalex_id") or "")))
    return deduped


def _decision_rank(decision: str) -> int:
    return {
        "include": 5,
        "preprint_watchlist": 4,
        "tertiary_background": 3,
        "needs_fulltext": 2,
        "borderline": 1,
        "exclude": 0,
    }.get(decision, -1)


def _merge_metadata_rows(
    existing_rows: Sequence[Mapping[str, Any]],
    additional_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return _deduplicate_metadata_rows([*existing_rows, *additional_rows])


def _merge_screening_rows(
    existing_rows: Sequence[Mapping[str, Any]],
    additional_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {
        str(row.get("record_id") or _candidate_row_key(row)): dict(row)
        for row in existing_rows
        if str(row.get("record_id") or _candidate_row_key(row))
    }
    for row in additional_rows:
        key = str(row.get("record_id") or _candidate_row_key(row))
        if not key:
            continue
        incoming = dict(row)
        current = merged.get(key)
        if current is None:
            merged[key] = incoming
            continue
        stronger = incoming if _decision_rank(str(incoming.get("machine_decision") or "")) > _decision_rank(str(current.get("machine_decision") or "")) else current
        weaker = current if stronger is incoming else incoming
        stronger["query_pack_ids"] = _merge_multi_value(current.get("query_pack_ids"), incoming.get("query_pack_ids"))
        stronger["anchor_match_ids"] = _merge_multi_value(current.get("anchor_match_ids"), incoming.get("anchor_match_ids"))
        stronger["negative_sentinel_match"] = _merge_multi_value(current.get("negative_sentinel_match"), incoming.get("negative_sentinel_match"))
        stronger["machine_reason"] = " | ".join(
            unique_preserve_order(value for value in [current.get("machine_reason", ""), incoming.get("machine_reason", "")] if value)
        )
        stronger_confidence = max(
            float(current.get("machine_confidence", 0) or 0),
            float(incoming.get("machine_confidence", 0) or 0),
        )
        stronger["machine_confidence"] = f"{stronger_confidence:.2f}" if stronger_confidence else ""
        stronger["resolved_decision"] = stronger.get("resolved_decision") or stronger.get("machine_decision", "")
        stronger["resolved_reason"] = stronger.get("resolved_reason") or weaker.get("resolved_reason") or ""
        stronger["notes"] = " | ".join(
            unique_preserve_order(value for value in [current.get("notes", ""), incoming.get("notes", "")] if value)
        )
        merged[key] = stronger
    rows = list(merged.values())
    rows.sort(key=lambda row: (str(row.get("title") or ""), str(row.get("openalex_id") or "")))
    return rows


def _screening_fulltext_defaults(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **dict(row),
        "pdf_local_path": "",
        "pdf_parse_status": "not_attempted",
        "fulltext_sections_seen": "",
        "equations_detected": "",
        "figures_detected": "",
    }


def _fulltext_candidate_rows(
    metadata_rows: Sequence[Mapping[str, Any]],
    screening_ft_rows: Sequence[Mapping[str, Any]],
    normalized_lookup: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen_record_ids: set[str] = set()

    for row in metadata_rows:
        if row.get("track") != "scholarly_core":
            continue
        record_id = str(row.get("record_id") or "")
        if not record_id or record_id in seen_record_ids:
            continue
        selected.append(dict(row))
        seen_record_ids.add(record_id)

    for row in screening_ft_rows:
        decision = str(row.get("resolved_decision") or row.get("machine_decision") or "")
        if decision != "needs_fulltext":
            continue
        record_id = str(row.get("record_id") or "")
        if not record_id or record_id in seen_record_ids:
            continue
        openalex_id = str(row.get("openalex_id") or "")
        normalized = dict(normalized_lookup.get(openalex_id, {}))
        selected.append(
            {
                "record_id": record_id,
                "openalex_id": openalex_id,
                "doi": row.get("doi", ""),
                "title": row.get("title", ""),
                "track": "needs_fulltext_queue",
                "best_pdf_url": normalized.get("best_pdf_url", row.get("best_pdf_url", "")),
            }
        )
        seen_record_ids.add(record_id)
    return selected


async def stage_dedup(ctx: PipelineContext) -> None:
    normalized_rows = _normalized_rows(ctx)
    if not normalized_rows:
        raise RuntimeError("Normalized candidates are missing. Run `scisieve normalize` first.")

    row_by_openalex_id: dict[str, dict[str, Any]] = {
        str(row.get("openalex_id") or ""): dict(row)
        for row in normalized_rows
        if str(row.get("openalex_id") or "")
    }
    core_candidates = [
        _row_to_research_work(ctx, row, source="OpenAlex Query Packs", track="core")
        for row in normalized_rows
        if str(row.get("type") or "") in {"article", "proceedings-article"}
    ]
    preprint_candidates = [
        _row_to_research_work(ctx, row, source="OpenAlex Query Packs", track="preprint_watchlist")
        for row in normalized_rows
        if str(row.get("type") or "") == "preprint"
    ]
    dedup_stats = _deduplicate_tracks(core_candidates, preprint_candidates)
    async with OpenAlexClient(
        email=ctx.config.contact_email,
        requests_per_second=7.0,
        api_key=ctx.config.openalex_api_key,
    ) as openalex:
        resolution_result = await _resolve_preprints_against_core(
            ctx,
            openalex,
            core_works=dedup_stats.core_works,
            watchlist_works=dedup_stats.watchlist_works,
        )

    for row in resolution_result.match_candidate_rows:
        candidate_openalex_id = row["candidate_openalex_id"]
        if candidate_openalex_id and candidate_openalex_id not in row_by_openalex_id:
            matched_rows = [
                item
                for item in resolution_result.core_works
                if item.openalex_id == candidate_openalex_id
            ]
            if matched_rows:
                matched = matched_rows[0]
                row_by_openalex_id[candidate_openalex_id] = {
                    "openalex_id": matched.openalex_id or "",
                    "doi": matched.doi or "",
                    "title": matched.title,
                    "publication_year": matched.publication_year or "",
                    "publication_date": matched.publication_date or "",
                    "type": matched.work_type,
                    "query_pack_ids": "",
                    "query_terms_triggered": "",
                    "cited_by_count": matched.cited_by_count or 0,
                    "best_pdf_url": matched.best_pdf_url or "",
                    "primary_source_name": "",
                    "retrieval_score_norm": "",
                    "source": "OpenAlex Title Resolution",
                    "matched_archival_id": matched.openalex_id or "",
                    "matched_archival_doi": matched.doi or "",
                    "raw_work": {},
                }

    deduped_payload = {
        "core": [work.model_dump(by_alias=True) for work in resolution_result.core_works],
        "watchlist": [work.model_dump(by_alias=True) for work in resolution_result.watchlist_works],
    }
    write_json(ctx.config.paths.normalized_dir / "deduped_candidates.json", deduped_payload)
    write_csv(ctx.config.paths.run_root / "preprint_resolution.csv", resolution_result.resolution_rows, PREPRINT_RESOLUTION_COLUMNS)
    write_csv(
        ctx.config.paths.run_root / "preprint_match_candidates.csv",
        resolution_result.match_candidate_rows,
        PREPRINT_MATCH_CANDIDATES_COLUMNS,
    )

    dedup_summary = {
        "raw_core_retrieved": sum(1 for row in normalized_rows if str(row.get("type") or "") in {"article", "proceedings-article"}),
        "raw_preprint_retrieved": sum(1 for row in normalized_rows if str(row.get("type") or "") == "preprint"),
        "included_topical_core_raw": sum(
            1
            for row in normalized_rows
            if str(row.get("type") or "") in {"article", "proceedings-article"}
            and evaluate_protocol(
                " ".join((str(row.get("title") or ""), str(row.get("abstract_text_reconstructed") or ""))).strip(),
                compiled_groups=ctx.compiled_groups,
                compiled_exclusions=ctx.compiled_exclusions,
            )[0]
        ),
        "included_topical_preprint_raw": sum(
            1
            for row in normalized_rows
            if str(row.get("type") or "") == "preprint"
            and evaluate_protocol(
                " ".join((str(row.get("title") or ""), str(row.get("abstract_text_reconstructed") or ""))).strip(),
                compiled_groups=ctx.compiled_groups,
                compiled_exclusions=ctx.compiled_exclusions,
            )[0]
        ),
        "core_duplicates_removed_by_doi_or_title": dedup_stats.core_duplicates_removed_by_doi_or_title,
        "preprint_duplicates_removed_by_doi_or_title": dedup_stats.preprint_duplicates_removed_by_doi_or_title,
        "preprint_duplicates_removed_against_core": dedup_stats.preprint_duplicates_removed_against_core,
        "preprints_resolved_to_existing_core": resolution_result.resolved_to_existing_core,
        "preprints_resolved_to_new_archival": resolution_result.resolved_to_new_archival,
        "core_after_dedup": sum(1 for work in resolution_result.core_works if _is_topical_work(ctx, work)),
        "preprint_watchlist_after_dedup": sum(1 for work in resolution_result.watchlist_works if _is_topical_work(ctx, work)),
    }
    write_csv(ctx.config.paths.run_root / "dedup_summary.csv", [dedup_summary], list(dedup_summary))


def _dedup_state(ctx: PipelineContext) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    payload_path = ctx.config.paths.normalized_dir / "deduped_candidates.json"
    if not payload_path.exists():
        raise RuntimeError("Dedup stage output is missing. Run `scisieve dedup` first.")
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    core = payload.get("core", [])
    watchlist = payload.get("watchlist", [])
    return (
        [row for row in core if isinstance(row, dict)],
        [row for row in watchlist if isinstance(row, dict)],
    )


def _work_screening_text(work: ResearchWork) -> str:
    return " ".join(part for part in (work.title, work.abstract or "") if part).strip()


def _is_topical_work(ctx: PipelineContext, work: ResearchWork) -> bool:
    return evaluate_protocol(
        _work_screening_text(work),
        compiled_groups=ctx.compiled_groups,
        compiled_exclusions=ctx.compiled_exclusions,
    )[0]


def _dedup_summary_from_state(ctx: PipelineContext) -> dict[str, int]:
    normalized_rows = _normalized_rows(ctx)
    core_rows, watchlist_rows = _dedup_state(ctx)
    existing_summary_rows = read_csv(ctx.config.paths.run_root / "dedup_summary.csv")
    existing_summary = existing_summary_rows[0] if existing_summary_rows else {}
    core_works = [_row_to_research_work(ctx, row, source=str(row.get("source") or "OpenAlex Query Packs"), track="core") for row in core_rows]
    watchlist_works = [
        _row_to_research_work(ctx, row, source=str(row.get("source") or "OpenAlex Query Packs"), track="preprint_watchlist")
        for row in watchlist_rows
    ]
    return {
        "raw_core_retrieved": sum(1 for row in normalized_rows if str(row.get("type") or "") in {"article", "proceedings-article"}),
        "raw_preprint_retrieved": sum(1 for row in normalized_rows if str(row.get("type") or "") == "preprint"),
        "included_topical_core_raw": sum(
            1
            for row in normalized_rows
            if str(row.get("type") or "") in {"article", "proceedings-article"}
            and evaluate_protocol(
                " ".join((str(row.get("title") or ""), str(row.get("abstract_text_reconstructed") or ""))).strip(),
                compiled_groups=ctx.compiled_groups,
                compiled_exclusions=ctx.compiled_exclusions,
            )[0]
        ),
        "included_topical_preprint_raw": sum(
            1
            for row in normalized_rows
            if str(row.get("type") or "") == "preprint"
            and evaluate_protocol(
                " ".join((str(row.get("title") or ""), str(row.get("abstract_text_reconstructed") or ""))).strip(),
                compiled_groups=ctx.compiled_groups,
                compiled_exclusions=ctx.compiled_exclusions,
            )[0]
        ),
        "core_duplicates_removed_by_doi_or_title": int(existing_summary.get("core_duplicates_removed_by_doi_or_title", 0) or 0),
        "preprint_duplicates_removed_by_doi_or_title": int(existing_summary.get("preprint_duplicates_removed_by_doi_or_title", 0) or 0),
        "preprint_duplicates_removed_against_core": int(existing_summary.get("preprint_duplicates_removed_against_core", 0) or 0),
        "preprints_resolved_to_existing_core": int(existing_summary.get("preprints_resolved_to_existing_core", 0) or 0),
        "preprints_resolved_to_new_archival": int(existing_summary.get("preprints_resolved_to_new_archival", 0) or 0),
        "core_after_dedup": sum(1 for work in core_works if _is_topical_work(ctx, work)),
        "preprint_watchlist_after_dedup": sum(1 for work in watchlist_works if _is_topical_work(ctx, work)),
    }


def _lookup_normalized_row(ctx: PipelineContext) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for row in _normalized_rows(ctx):
        openalex_id = str(row.get("openalex_id") or "")
        if openalex_id:
            mapping[openalex_id] = row
    snowball_raw_path = ctx.config.paths.normalized_dir / "snowball_included_raw.jsonl"
    if snowball_raw_path.exists():
        for row in read_jsonl(snowball_raw_path):
            openalex_id = str(row.get("openalex_id") or "")
            if openalex_id:
                mapping[openalex_id] = row
    return mapping


def stage_screen_ta(ctx: PipelineContext) -> None:
    core_rows, watchlist_rows = _dedup_state(ctx)
    write_csv(
        ctx.config.paths.run_root / "dedup_summary.csv",
        [_dedup_summary_from_state(ctx)],
        [
            "raw_core_retrieved",
            "raw_preprint_retrieved",
            "included_topical_core_raw",
            "included_topical_preprint_raw",
            "core_duplicates_removed_by_doi_or_title",
            "preprint_duplicates_removed_by_doi_or_title",
            "preprint_duplicates_removed_against_core",
            "preprints_resolved_to_existing_core",
            "preprints_resolved_to_new_archival",
            "core_after_dedup",
            "preprint_watchlist_after_dedup",
        ],
    )
    normalized_lookup = _lookup_normalized_row(ctx)
    screening_rows: list[dict[str, Any]] = []
    screening_ft_rows: list[dict[str, Any]] = []
    metadata_rows: list[dict[str, Any]] = []
    borderline_rows: list[dict[str, Any]] = []
    randomizer = random.Random(ctx.config.profile.review.double_screen_seed)

    all_rows = [*core_rows, *watchlist_rows]
    for row in all_rows:
        openalex_id = str(row.get("openalex_id") or "")
        normalized_row = normalized_lookup.get(openalex_id, {})
        merged_row = {**normalized_row, **row}
        record_id = _record_id_for_work(openalex_id, row.get("doi"), str(row.get("title") or ""))
        classifier = _topic_classifier(ctx, merged_row)
        screening_row = _screening_row(record_id, merged_row, classifier)
        screening_rows.append(screening_row)
        screening_ft_rows.append(_screening_fulltext_defaults(screening_row))

        final_track = ""
        if classifier["machine_decision"] == "include":
            final_track = "scholarly_core"
        elif classifier["machine_decision"] == "tertiary_background":
            final_track = "scholarly_tertiary"
        elif classifier["machine_decision"] == "preprint_watchlist":
            final_track = "preprint_watchlist"

        if classifier["machine_decision"] in {"borderline", "needs_fulltext"}:
            borderline_rows.append(screening_row)

        if final_track:
            metadata_rows.append(
                {
                    "record_id": record_id,
                    "track": final_track,
                    "archival_status": row.get("archival_status", ""),
                    "source": row.get("source", "OpenAlex Query Packs"),
                    "discovery_mode": "query_packs",
                    "query_pack_ids": normalized_row.get("query_pack_ids", ""),
                    "query_terms_triggered": normalized_row.get("query_terms_triggered", ""),
                    "anchor_match_ids": classifier.get("anchor_match_ids", ""),
                    "negative_sentinel_match": classifier.get("negative_sentinel_match", ""),
                    "openalex_id": openalex_id,
                    "doi": row.get("doi", ""),
                    "title": row.get("title", ""),
                    "publication_year": row.get("publication_year", ""),
                    "publication_date": row.get("publication_date", ""),
                    "type": row.get("type", ""),
                    "primary_source_name": normalized_row.get("primary_source_name", ""),
                    "cited_by_count": row.get("cited_by_count", ""),
                    "best_pdf_url": row.get("best_pdf_url", ""),
                    "label_primary_dimension": row.get("label_primary_dimension", ""),
                    "label_primary_value": row.get("label_primary_value", ""),
                    "label_secondary_dimension": row.get("label_secondary_dimension", ""),
                    "label_secondary_value": row.get("label_secondary_value", ""),
                    "topic_labels_json": row.get("topic_labels_json", ""),
                    "machine_decision": classifier.get("machine_decision", ""),
                    "machine_confidence": classifier.get("machine_confidence", ""),
                    "machine_reason": classifier.get("machine_reason", ""),
                    "matched_archival_id": row.get("matched_archival_id", ""),
                    "matched_archival_doi": row.get("matched_archival_doi", ""),
                    "retrieval_score_norm": normalized_row.get("retrieval_score_norm", ""),
                }
            )

    sample_size = min(
        len(screening_rows),
        max(1, int(math.ceil(len(screening_rows) * ctx.config.profile.review.double_screen_fraction))),
    )
    double_screen_sample = randomizer.sample(screening_rows, sample_size) if screening_rows else []

    write_csv(ctx.config.paths.run_root / "screening_title_abstract.csv", screening_rows, SCREENING_TA_COLUMNS)
    write_csv(ctx.config.paths.run_root / "screening_fulltext.csv", screening_ft_rows, SCREENING_FT_COLUMNS)
    write_csv(ctx.config.paths.run_root / "review_queue_borderline.csv", borderline_rows, SCREENING_TA_COLUMNS)
    write_csv(ctx.config.paths.run_root / "review_queue_conflicts.csv", [], SCREENING_TA_COLUMNS)
    write_csv(ctx.config.paths.run_root / "double_screen_sample.csv", double_screen_sample, SCREENING_TA_COLUMNS)
    write_csv(ctx.config.paths.run_root / "metadata.csv", metadata_rows, METADATA_COLUMNS)
    write_markdown(
        ctx.config.paths.run_root / "kappa_report.md",
        "# Kappa Report\n\nNo reviewer decisions are populated yet. Cohen's kappa will be computed once reviewer columns are filled.\n",
    )
    _write_pack_reports(ctx)
    _write_prisma_counts(ctx)


def _write_prisma_counts(ctx: PipelineContext) -> None:
    normalized_rows = _normalized_rows(ctx)
    metadata_rows = read_csv(ctx.config.paths.run_root / "metadata.csv")
    dedup_summary_rows = read_csv(ctx.config.paths.run_root / "dedup_summary.csv")
    dedup_summary = dedup_summary_rows[0] if dedup_summary_rows else {}
    counts = {
        "retrieved_core_raw": sum(1 for row in normalized_rows if str(row.get("type") or "") in {"article", "proceedings-article"}),
        "retrieved_preprint_raw": sum(1 for row in normalized_rows if str(row.get("type") or "") == "preprint"),
        "included_topical_core_raw": int(dedup_summary.get("included_topical_core_raw", 0) or 0),
        "included_topical_preprint_raw": int(dedup_summary.get("included_topical_preprint_raw", 0) or 0),
        "core_after_dedup": int(dedup_summary.get("core_after_dedup", 0) or 0),
        "preprint_watchlist_after_dedup": int(dedup_summary.get("preprint_watchlist_after_dedup", 0) or 0),
        "preprints_resolved_to_existing_core": int(dedup_summary.get("preprints_resolved_to_existing_core", 0) or 0),
        "preprints_resolved_to_new_archival": int(dedup_summary.get("preprints_resolved_to_new_archival", 0) or 0),
        "final_export_core": sum(1 for row in metadata_rows if row.get("track") == "scholarly_core"),
        "final_export_preprint_watchlist": sum(1 for row in metadata_rows if row.get("track") == "preprint_watchlist"),
    }
    write_csv(
        ctx.config.paths.run_root / "prisma_counts.csv",
        [{"stage": stage, "count": counts.get(stage, 0)} for stage in PRISMA_STAGE_ORDER],
        ["stage", "count"],
    )


def _write_pack_reports(ctx: PipelineContext) -> None:
    raw_entries = _load_raw_entries(ctx)
    normalized_rows = _normalized_rows(ctx)
    metadata_rows = read_csv(ctx.config.paths.run_root / "metadata.csv")
    screening_rows = read_csv(ctx.config.paths.run_root / "screening_title_abstract.csv")
    coverage_rows = read_csv(ctx.config.paths.run_root / "coverage_anchor_results.csv")
    negative_rows = read_csv(ctx.config.paths.run_root / "negative_sentinel_report.csv")

    raw_hits_by_pack = Counter(str(entry.get("query_pack_id") or "") for entry in raw_entries)
    metadata_by_pack = Counter()
    tertiary_by_pack = Counter()
    ta_by_pack = Counter()
    after_negative = Counter()
    after_metadata = Counter()
    after_dedup = Counter()
    anchor_hits_by_pack = Counter()
    negative_hits_by_pack = Counter()

    for row in normalized_rows:
        packs = [value for value in str(row.get("query_pack_ids") or "").split(";") if value]
        if _metadata_filter_pass(row.get("raw_work", {}) or row, ctx):
            for pack in packs:
                after_metadata[pack] += 1
        text = " ".join((str(row.get("title") or ""), str(row.get("abstract_text_reconstructed") or ""))).strip().lower()
        if not any(term in text for term in _pack_negative_exclusions(ctx)):
            for pack in packs:
                after_negative[pack] += 1
    for row in screening_rows:
        if "snowball_round=" in str(row.get("notes") or ""):
            continue
        packs = [value for value in str(row.get("query_pack_ids") or "").split(";") if value]
        if row.get("machine_decision") != "exclude":
            for pack in packs:
                ta_by_pack[pack] += 1
    for row in metadata_rows:
        if "snowball" in _split_multi_value(row.get("discovery_mode")):
            continue
        packs = [value for value in str(row.get("query_pack_ids") or "").split(";") if value]
        for pack in packs:
            after_dedup[pack] += 1
            if row.get("track") == "scholarly_core":
                metadata_by_pack[pack] += 1
            if row.get("track") == "scholarly_tertiary":
                tertiary_by_pack[pack] += 1
    for row in coverage_rows:
        if row.get("polarity") != "positive" or row.get("retrieved") != "True":
            continue
        for pack in str(row.get("retrieved_pack_ids") or "").split(";"):
            if pack:
                anchor_hits_by_pack[pack] += 1
    for row in negative_rows:
        if row.get("retrieved") != "True":
            continue
        for pack in str(row.get("retrieved_pack_ids") or "").split(";"):
            if pack:
                negative_hits_by_pack[pack] += 1

    pack_rows: list[dict[str, Any]] = []
    pack_ids = [str(pack.get("id") or "") for pack in _pack_definitions(ctx)]
    for pack_id in pack_ids:
        raw_hits = raw_hits_by_pack.get(pack_id, 0)
        included_core = metadata_by_pack.get(pack_id, 0)
        pack_rows.append(
            {
                "pack_id": pack_id,
                "raw_hits": raw_hits,
                "after_metadata_filter": after_metadata.get(pack_id, 0),
                "after_negative_exclusions": after_negative.get(pack_id, 0),
                "after_dedup": after_dedup.get(pack_id, 0),
                "after_title_abstract_screen": ta_by_pack.get(pack_id, 0),
                "after_fulltext_screen": ta_by_pack.get(pack_id, 0),
                "included_core": included_core,
                "included_tertiary": tertiary_by_pack.get(pack_id, 0),
                "precision_proxy": f"{(included_core / raw_hits):.4f}" if raw_hits else "0.0000",
                "anchor_hits": anchor_hits_by_pack.get(pack_id, 0),
                "negative_sentinel_hits": negative_hits_by_pack.get(pack_id, 0),
            }
        )
    write_csv(ctx.config.paths.run_root / "retrieval_yield_by_pack.csv", pack_rows, RETRIEVAL_YIELD_COLUMNS)

    pack_sets = [[pack for pack in str(row.get("query_pack_ids") or "").split(";") if pack] for row in normalized_rows]
    overlap_rows: list[dict[str, Any]] = []
    unique_rows: list[dict[str, Any]] = []
    for left_pack in pack_ids:
        for right_pack in pack_ids:
            overlap = sum(1 for packs in pack_sets if left_pack in packs and right_pack in packs)
            overlap_rows.append({"left_pack_id": left_pack, "right_pack_id": right_pack, "overlap_count": overlap})
        unique_rows.append({"pack_id": left_pack, "unique_hits": sum(1 for packs in pack_sets if packs == [left_pack])})
    write_csv(ctx.config.paths.run_root / "pack_overlap_matrix.csv", overlap_rows, PACK_OVERLAP_COLUMNS)
    write_csv(ctx.config.paths.run_root / "unique_hits_by_pack.csv", unique_rows, UNIQUE_HITS_COLUMNS)

    missing_anchor_rows: list[dict[str, Any]] = []
    hint_map = _query_pack_hint_map(ctx)
    coverage_by_anchor = {row.get("anchor_id", ""): row for row in coverage_rows}
    for row in _positive_anchor_rows(ctx):
        anchor_id = row.get("anchor_id", "")
        coverage = coverage_by_anchor.get(anchor_id, {})
        if coverage.get("retrieved") == "True":
            continue
        missing_anchor_rows.append(
            {
                "pack_id": hint_map.get(anchor_id, ""),
                "anchor_id": anchor_id,
                "anchor_title": row.get("anchor_title", ""),
                "tier": row.get("tier", ""),
                "expected_stage": row.get("expected_stage", ""),
            }
        )
    write_csv(ctx.config.paths.run_root / "top_missing_anchors_by_pack.csv", missing_anchor_rows, TOP_MISSING_ANCHORS_COLUMNS)


def _quality_score_from_hits(strength: int) -> tuple[int, float]:
    if strength >= 4:
        return 2, 0.88
    if strength >= 2:
        return 1, 0.68
    return 0, 0.52


async def stage_snowball(ctx: PipelineContext) -> None:
    if not ctx.config.profile.snowball.enabled:
        write_csv(ctx.config.paths.run_root / "citation_seed_set.csv", [], ["openalex_id", "doi"])
        write_csv(ctx.config.paths.run_root / "snowball_rounds.csv", [], SNOWBALL_ROUNDS_COLUMNS)
        write_csv(ctx.config.paths.run_root / "snowball_edges.csv", [], SNOWBALL_EDGES_COLUMNS)
        write_csv(ctx.config.paths.run_root / "snowball_candidates.csv", [], SNOWBALL_CANDIDATES_COLUMNS)
        write_csv(ctx.config.paths.run_root / "snowball_included.csv", [], METADATA_COLUMNS)
        write_csv(ctx.config.paths.run_root / "snowball_excluded.csv", [], SCREENING_TA_COLUMNS)
        write_jsonl(ctx.config.paths.normalized_dir / "snowball_included_raw.jsonl", [])
        write_markdown(ctx.config.paths.run_root / "seed_expansion_report.md", "# Seed Expansion Report\n\nSnowballing disabled for this profile.\n")
        return

    metadata_rows = read_csv(ctx.config.paths.run_root / "metadata.csv")
    if not metadata_rows:
        raise RuntimeError("Metadata is missing. Run `scisieve screen-ta` first.")
    screening_rows_existing = read_csv(ctx.config.paths.run_root / "screening_title_abstract.csv")
    screening_ft_rows_existing = read_csv(ctx.config.paths.run_root / "screening_fulltext.csv")

    positive_anchors = _positive_anchor_rows(ctx)
    seed_records: list[dict[str, str]] = []
    discovered_by_doi = {normalize_doi(row.get("doi")): row for row in metadata_rows if normalize_doi(row.get("doi"))}
    async with (
        OpenAlexClient(
            email=ctx.config.contact_email,
            requests_per_second=7.0,
            api_key=ctx.config.openalex_api_key,
        ) as openalex,
        OpenCitationsClient(email=ctx.config.contact_email, requests_per_second=3.0) as opencitations,
    ):
        for anchor in positive_anchors:
            anchor_doi = normalize_doi(anchor.get("doi"))
            if anchor_doi and anchor_doi in discovered_by_doi:
                matched_row = discovered_by_doi[anchor_doi]
                openalex_id = matched_row.get("openalex_id", "")
                if openalex_id:
                    seed_records.append(
                        {
                            "openalex_id": openalex_id,
                            "doi": normalize_doi(matched_row.get("doi")) or anchor_doi,
                            "query_pack_ids": matched_row.get("query_pack_ids", "") or anchor.get("query_pack_hint", ""),
                        }
                    )
                continue
            if anchor_doi:
                work = await openalex.lookup_work_by_doi(anchor_doi)
                if work and work.get("id"):
                    seed_records.append(
                        {
                            "openalex_id": str(work["id"]),
                            "doi": normalize_doi(work.get("doi")) or anchor_doi,
                            "query_pack_ids": anchor.get("query_pack_hint", ""),
                        }
                    )
        for row in metadata_rows:
            if row.get("track") != "scholarly_core":
                continue
            openalex_id = str(row.get("openalex_id") or "")
            if openalex_id:
                seed_records.append(
                    {
                        "openalex_id": openalex_id,
                        "doi": normalize_doi(row.get("doi")) or "",
                        "query_pack_ids": row.get("query_pack_ids", ""),
                    }
                )
        deduped_seed_records: list[dict[str, str]] = []
        seed_index: dict[str, dict[str, str]] = {}
        for row in seed_records:
            key = row.get("openalex_id") or row.get("doi") or ""
            if not key:
                continue
            if key not in seed_index:
                seed_index[key] = dict(row)
                deduped_seed_records.append(seed_index[key])
                continue
            seed_index[key]["doi"] = seed_index[key].get("doi") or row.get("doi") or ""
            seed_index[key]["query_pack_ids"] = _merge_multi_value(
                seed_index[key].get("query_pack_ids", ""),
                row.get("query_pack_ids", ""),
            )
        seed_records = deduped_seed_records[: ctx.config.profile.snowball.max_seeds]
        write_csv(
            ctx.config.paths.run_root / "citation_seed_set.csv",
            [{"openalex_id": row.get("openalex_id", ""), "doi": row.get("doi", "")} for row in seed_records],
            ["openalex_id", "doi"],
        )

        zero_yield_rounds = 0
        seen_ids = {
            str(row.get("openalex_id") or "")
            for row in metadata_rows
            if str(row.get("openalex_id") or "")
        }
        round_rows: list[dict[str, Any]] = []
        edge_rows: list[dict[str, Any]] = []
        candidate_rows: list[dict[str, Any]] = []
        included_rows: list[dict[str, Any]] = []
        excluded_rows: list[dict[str, Any]] = []
        included_raw_rows: list[dict[str, Any]] = []
        screening_rows_new: list[dict[str, Any]] = []
        next_seeds = list(seed_records)
        seed_pack_ids_map = {
            str(row.get("openalex_id") or ""): row.get("query_pack_ids", "")
            for row in seed_records
            if str(row.get("openalex_id") or "")
        }
        anchor_titles = [item.get("anchor_title", "") for item in positive_anchors]

        for round_index in range(1, ctx.config.profile.snowball.max_rounds + 1):
            merged: dict[str, dict[str, Any]] = {}
            directions: dict[str, set[str]] = defaultdict(set)
            candidate_seed_ids: dict[str, set[str]] = defaultdict(set)
            backward_count = 0
            forward_count = 0

            def add_candidates(seed_id: str, candidates: Sequence[Mapping[str, Any]], direction: str) -> None:
                for candidate in candidates:
                    candidate_id = str(candidate.get("id") or "")
                    if not candidate_id:
                        continue
                    merged[candidate_id] = dict(candidate)
                    directions[candidate_id].add(direction)
                    candidate_seed_ids[candidate_id].add(seed_id)
                    edge_rows.append(
                        {
                            "round_id": round_index,
                            "seed_id": seed_id,
                            "candidate_id": candidate_id,
                            "direction": direction,
                        }
                    )

            async def _safe_backward_openalex(seed_id: str) -> tuple[str, list[dict[str, Any]]]:
                try:
                    return (
                        seed_id,
                        await backward_snowball(
                            openalex,
                            [seed_id],
                            max_refs_per_seed=ctx.config.profile.snowball.backward_refs_per_seed,
                            max_total_refs=ctx.config.profile.snowball.backward_refs_per_seed,
                        ),
                    )
                except Exception:
                    return seed_id, []

            async def _safe_forward_openalex(seed_id: str) -> tuple[str, list[dict[str, Any]]]:
                try:
                    return (
                        seed_id,
                        await forward_snowball(
                            openalex,
                            [seed_id],
                            max_records_per_seed=ctx.config.profile.snowball.forward_max_per_seed,
                        ),
                    )
                except Exception:
                    return seed_id, []

            async def _safe_backward_opencitations(seed_id: str, seed_doi: str) -> tuple[str, list[dict[str, Any]]]:
                try:
                    return (
                        seed_id,
                        await backward_snowball_opencitations(
                            openalex,
                            opencitations,
                            [seed_doi],
                            max_refs_per_seed=ctx.config.profile.snowball.backward_refs_per_seed,
                        ),
                    )
                except Exception:
                    return seed_id, []

            async def _safe_forward_opencitations(seed_id: str, seed_doi: str) -> tuple[str, list[dict[str, Any]]]:
                try:
                    return (
                        seed_id,
                        await forward_snowball_opencitations(
                            openalex,
                            opencitations,
                            [seed_doi],
                            max_records_per_seed=ctx.config.profile.snowball.forward_max_per_seed,
                        ),
                    )
                except Exception:
                    return seed_id, []

            seed_ids = [str(seed.get("openalex_id") or "") for seed in next_seeds if str(seed.get("openalex_id") or "")]
            seed_doi_pairs = [
                (str(seed.get("openalex_id") or ""), normalize_doi(seed.get("doi")) or "")
                for seed in next_seeds
                if str(seed.get("openalex_id") or "") and normalize_doi(seed.get("doi"))
            ]

            openalex_backward_batches = await asyncio.gather(*(_safe_backward_openalex(seed_id) for seed_id in seed_ids))
            openalex_forward_batches = await asyncio.gather(*(_safe_forward_openalex(seed_id) for seed_id in seed_ids))
            opencitations_backward_batches = await asyncio.gather(
                *(_safe_backward_opencitations(seed_id, seed_doi) for seed_id, seed_doi in seed_doi_pairs)
            )
            opencitations_forward_batches = await asyncio.gather(
                *(_safe_forward_opencitations(seed_id, seed_doi) for seed_id, seed_doi in seed_doi_pairs)
            )

            for seed_id, backward in openalex_backward_batches:
                backward_count += len(backward)
                add_candidates(seed_id, backward, "backward_openalex")
            for seed_id, forward in openalex_forward_batches:
                forward_count += len(forward)
                add_candidates(seed_id, forward, "forward_openalex")
            for seed_id, backward_oc in opencitations_backward_batches:
                backward_count += len(backward_oc)
                add_candidates(seed_id, backward_oc, "backward_opencitations")
            for seed_id, forward_oc in opencitations_forward_batches:
                forward_count += len(forward_oc)
                add_candidates(seed_id, forward_oc, "forward_opencitations")

            deduped_candidates = [candidate for cid, candidate in merged.items() if cid and cid not in seen_ids]
            included_this_round: list[dict[str, str]] = []
            topical_pass_count = 0
            ta_pass_count = 0

            for candidate in deduped_candidates:
                candidate_id = str(candidate.get("id") or "")
                candidate_doi = normalize_doi(candidate.get("doi")) or ""
                primary_source_name, _, _, _ = _primary_source_metadata(candidate)
                best_pdf_url = candidate.get("best_oa_location", {}).get("pdf_url") if isinstance(candidate.get("best_oa_location"), dict) else ""
                if not best_pdf_url:
                    primary_location = candidate.get("primary_location")
                    if isinstance(primary_location, dict):
                        best_pdf_url = str(primary_location.get("pdf_url") or "")
                seed_pack_ids = unique_preserve_order(
                    pack_id
                    for seed_id in sorted(candidate_seed_ids.get(candidate_id, set()))
                    for pack_id in _split_multi_value(seed_pack_ids_map.get(seed_id, ""))
                )
                row = {
                    "openalex_id": candidate_id,
                    "doi": candidate_doi,
                    "title": str(candidate.get("title") or ""),
                    "publication_year": candidate.get("publication_year") or "",
                    "publication_date": candidate.get("publication_date") or "",
                    "type": candidate.get("type") or "",
                    "abstract_text_reconstructed": build_screening_text({"title": "", "abstract_inverted_index": candidate.get("abstract_inverted_index")}),
                    "query_pack_ids": ";".join(seed_pack_ids),
                    "query_terms_triggered": ";".join(
                        unique_preserve_order(
                            _term_hits(
                                " ".join(
                                    part
                                for part in (
                                    str(candidate.get("title") or ""),
                                    build_screening_text({"title": "", "abstract_inverted_index": candidate.get("abstract_inverted_index")}),
                                )
                                if part
                            ),
                                flatten_term_groups(_topic_term_groups(ctx)),
                            )
                        )
                    ),
                    "cited_by_count": candidate.get("cited_by_count") or 0,
                    "primary_source_name": primary_source_name,
                    "best_pdf_url": best_pdf_url or "",
                }
                classifier = _topic_classifier(ctx, row)
                if classifier["machine_decision"] == "include" and not _is_archival_work_type(row["type"]):
                    classifier = {
                        **classifier,
                        "machine_decision": "exclude",
                        "machine_reason": f"{classifier['machine_reason']}; non_archival_work_type={row['type']}",
                    }
                if classifier["topical_score"] > 0:
                    topical_pass_count += 1
                anchor_proximity = max(
                    (SequenceMatcher(None, normalize_title(row["title"]), normalize_title(title)).ratio() for title in anchor_titles if title),
                    default=0.0,
                )
                recentness = 0.0
                if row["publication_year"] and str(row["publication_year"]).isdigit():
                    recentness = max(0.0, min(1.0, (int(row["publication_year"]) - 2010) / 16.0))
                direction_value = directions.get(candidate_id, set())
                priority_score = (
                    min(5.0, classifier["topical_score"]) * 0.35
                    + len(direction_value) * 0.15
                    + anchor_proximity * 0.2
                    + recentness * 0.15
                    + (1.0 if candidate.get("type") in {"article", "proceedings-article"} else 0.0) * 0.15
                )
                decision = "exclude"
                if classifier["machine_decision"] in {"include", "tertiary_background"}:
                    decision = classifier["machine_decision"]
                    ta_pass_count += 1
                    if classifier["machine_decision"] == "include":
                        included_this_round.append({"openalex_id": candidate_id, "doi": candidate_doi})
                        seen_ids.add(candidate_id)
                elif priority_score >= 0.7 and classifier["machine_decision"] in {"needs_fulltext", "borderline"}:
                    decision = "needs_fulltext"
                    ta_pass_count += 1
                record_id = _record_id_for_work(candidate_id, row["doi"], row["title"])
                screening_row = _screening_row(record_id, row, {**classifier, "machine_decision": decision})
                screening_row["notes"] = f"snowball_round={round_index}; directions={'+'.join(sorted(direction_value))}"
                screening_rows_new.append(screening_row)
                candidate_rows.append(
                    {
                        "round_id": round_index,
                        "candidate_id": record_id,
                        "openalex_id": candidate_id,
                        "title": row["title"],
                        "doi": row["doi"] or "",
                        "edge_count_from_seeds": sum(1 for edge in edge_rows if edge["candidate_id"] == candidate_id and edge["round_id"] == round_index),
                        "seed_diversity": len({edge["seed_id"] for edge in edge_rows if edge["candidate_id"] == candidate_id and edge["round_id"] == round_index}),
                        "citation_direction": "+".join(sorted(direction_value)),
                        "anchor_proximity": f"{anchor_proximity:.4f}",
                        "title_abstract_topical_score": classifier["topical_score"],
                        "venue_signal": 1 if candidate.get("type") in {"article", "proceedings-article"} else 0,
                        "recentness_weight": f"{recentness:.4f}",
                        "priority_score": f"{priority_score:.4f}",
                        "decision": decision,
                    }
                )
                if decision == "include":
                    snowball_label_fields = _topic_label_fields(ctx, _topic_label_assignments(ctx, f"{row['title']} {row['abstract_text_reconstructed']}"))
                    included_rows.append(
                    {
                            "record_id": record_id,
                            "track": "scholarly_core",
                            "archival_status": "archival",
                            "source": "Snowballing",
                            "discovery_mode": "snowball",
                            "query_pack_ids": row["query_pack_ids"],
                            "query_terms_triggered": row["query_terms_triggered"],
                            "anchor_match_ids": classifier["anchor_match_ids"],
                            "negative_sentinel_match": classifier["negative_sentinel_match"],
                            "openalex_id": candidate_id,
                            "doi": row["doi"] or "",
                            "title": row["title"],
                            "publication_year": row["publication_year"],
                            "publication_date": row["publication_date"],
                            "type": row["type"],
                            "primary_source_name": row["primary_source_name"],
                            "cited_by_count": row["cited_by_count"],
                            "best_pdf_url": row["best_pdf_url"],
                            "label_primary_dimension": snowball_label_fields["label_primary_dimension"],
                            "label_primary_value": snowball_label_fields["label_primary_value"],
                            "label_secondary_dimension": snowball_label_fields["label_secondary_dimension"],
                            "label_secondary_value": snowball_label_fields["label_secondary_value"],
                            "topic_labels_json": snowball_label_fields.get("topic_labels_json", ""),
                            "machine_decision": "include",
                            "machine_confidence": classifier["machine_confidence"],
                            "machine_reason": classifier["machine_reason"],
                            "matched_archival_id": "",
                            "matched_archival_doi": "",
                            "retrieval_score_norm": f"{priority_score:.4f}",
                        }
                    )
                    included_raw_rows.append(
                        {
                            "candidate_id": record_id,
                            "source_name": "Snowballing",
                            "source_entity_id": candidate_id,
                            "openalex_id": candidate_id,
                            "doi": row["doi"] or "",
                            "title": row["title"],
                            "normalized_title": normalize_title(row["title"]),
                            "abstract_text_reconstructed": row["abstract_text_reconstructed"],
                            "publication_year": row["publication_year"],
                            "publication_date": row["publication_date"],
                            "type": row["type"],
                            "type_crossref": _type_crossref_from_work(candidate),
                            "language": candidate.get("language") or "",
                            "cited_by_count": row["cited_by_count"],
                            "indexed_in": "OpenAlex;OpenCitations" if any("opencitations" in item for item in direction_value) else "OpenAlex",
                            "primary_source_name": row["primary_source_name"],
                            "primary_source_type": "",
                            "primary_source_is_core": "",
                            "primary_source_is_in_doaj": "",
                            "best_pdf_url": row["best_pdf_url"],
                            "has_abstract": str(bool(candidate.get("abstract_inverted_index"))),
                            "has_fulltext": "False",
                            "query_pack_ids": row["query_pack_ids"],
                            "query_terms_triggered": row["query_terms_triggered"],
                            "retrieval_score_raw": f"{priority_score:.6f}",
                            "retrieval_score_norm": f"{priority_score:.6f}",
                            "retrieval_timestamp_utc": ctx.config.run_timestamp_utc,
                            "snapshot_id": ctx.config.profile.snapshot_id,
                            "record_hash": _hash_row(candidate),
                            "stream": "snowball",
                            "raw_work": candidate,
                        }
                    )
                else:
                    excluded_rows.append(screening_row)

            round_rows.append(
                {
                    "round_id": round_index,
                    "seed_count": len(next_seeds),
                    "backward_candidates": backward_count,
                    "forward_candidates": forward_count,
                    "after_dedup": len(deduped_candidates),
                    "after_topical_filter": topical_pass_count,
                    "after_ta_screen": ta_pass_count,
                    "after_ft_screen": ta_pass_count,
                    "new_core": len(included_this_round),
                }
            )
            if len(included_this_round) < ctx.config.profile.snowball.min_new_core_per_round:
                zero_yield_rounds += 1
            else:
                zero_yield_rounds = 0
            if zero_yield_rounds >= 2 or not included_this_round:
                break
            next_seeds = included_this_round
            for row in next_seeds:
                openalex_id = str(row.get("openalex_id") or "")
                if openalex_id and openalex_id not in seed_pack_ids_map:
                    matched_metadata = next(
                        (item for item in included_rows if item.get("openalex_id") == openalex_id),
                        {},
                    )
                    seed_pack_ids_map[openalex_id] = matched_metadata.get("query_pack_ids", "")

    write_csv(ctx.config.paths.run_root / "snowball_rounds.csv", round_rows, SNOWBALL_ROUNDS_COLUMNS)
    write_csv(ctx.config.paths.run_root / "snowball_edges.csv", edge_rows, SNOWBALL_EDGES_COLUMNS)
    write_csv(ctx.config.paths.run_root / "snowball_candidates.csv", candidate_rows, SNOWBALL_CANDIDATES_COLUMNS)
    write_csv(ctx.config.paths.run_root / "snowball_included.csv", included_rows, METADATA_COLUMNS)
    write_csv(ctx.config.paths.run_root / "snowball_excluded.csv", excluded_rows, SCREENING_TA_COLUMNS)
    write_jsonl(ctx.config.paths.normalized_dir / "snowball_included_raw.jsonl", included_raw_rows)
    merged_metadata = _merge_metadata_rows(metadata_rows, included_rows)
    merged_screening_rows = _merge_screening_rows(screening_rows_existing, screening_rows_new)
    merged_screening_ft_rows = _merge_screening_rows(
        screening_ft_rows_existing,
        [_screening_fulltext_defaults(row) for row in screening_rows_new],
    )
    write_csv(ctx.config.paths.run_root / "metadata.csv", merged_metadata, METADATA_COLUMNS)
    write_csv(ctx.config.paths.run_root / "screening_title_abstract.csv", merged_screening_rows, SCREENING_TA_COLUMNS)
    write_csv(ctx.config.paths.run_root / "screening_fulltext.csv", merged_screening_ft_rows, SCREENING_FT_COLUMNS)
    _write_prisma_counts(ctx)
    write_markdown(
        ctx.config.paths.run_root / "seed_expansion_report.md",
        "# Seed Expansion Report\n\n"
        f"- initial seeds: {len(seed_records)}\n"
        f"- rounds executed: {len(round_rows)}\n"
        f"- included via snowballing: {len(included_rows)}\n",
    )


async def stage_fulltext(ctx: PipelineContext) -> None:
    metadata_rows = read_csv(ctx.config.paths.run_root / "metadata.csv")
    normalized_lookup = _lookup_normalized_row(ctx)
    screening_ft_rows = read_csv(ctx.config.paths.run_root / "screening_fulltext.csv")
    screening_ft_by_id = {row["record_id"]: row for row in screening_ft_rows if row.get("record_id")}
    rows_to_process = _fulltext_candidate_rows(metadata_rows, screening_ft_rows, normalized_lookup)
    inventory_rows: list[dict[str, Any]] = []
    pdf_rows: list[dict[str, Any]] = []
    parse_rows: list[dict[str, Any]] = []

    if not ctx.config.profile.fulltext.enabled:
        for row in rows_to_process:
            inventory_rows.append(
                {
                    "record_id": row["record_id"],
                    "openalex_id": row["openalex_id"],
                    "doi": row["doi"],
                    "title": row["title"],
                    "track": row["track"],
                    "best_pdf_url": row["best_pdf_url"],
                    "download_attempted": "False",
                    "download_status": "disabled_by_profile",
                    "pdf_local_path": "",
                    "parse_status": "not_attempted",
                    "extraction_ready": "False",
                }
            )
        write_csv(ctx.config.paths.run_root / "fulltext_inventory.csv", inventory_rows, FULLTEXT_INVENTORY_COLUMNS)
        write_csv(ctx.config.paths.run_root / "pdf_download_log.csv", pdf_rows, PDF_DOWNLOAD_COLUMNS)
        write_csv(ctx.config.paths.run_root / "parse_log.csv", parse_rows, PARSE_LOG_COLUMNS)
        return

    works_for_extractor: list[dict[str, Any]] = []
    row_by_id: dict[str, dict[str, Any]] = {}
    for row in rows_to_process[: ctx.config.profile.fulltext.limit]:
        normalized = normalized_lookup.get(row.get("openalex_id", ""), {})
        raw_work = normalized.get("raw_work", {})
        if isinstance(raw_work, dict) and raw_work:
            works_for_extractor.append(raw_work)
            row_by_id[str(raw_work.get("id") or "")] = row
            continue
        inventory_rows.append(
            {
                "record_id": row.get("record_id", ""),
                "openalex_id": row.get("openalex_id", ""),
                "doi": row.get("doi", ""),
                "title": row.get("title", ""),
                "track": row.get("track", ""),
                "best_pdf_url": row.get("best_pdf_url", ""),
                "download_attempted": "False",
                "download_status": "no_raw_work",
                "pdf_local_path": "",
                "parse_status": "not_attempted",
                "extraction_ready": "False",
            }
        )

    async with PDFSectionExtractor(
        email=ctx.config.contact_email,
        requests_per_second=ctx.config.profile.fulltext.requests_per_second,
    ) as extractor:
        results = await extractor.process_many(
            works_for_extractor,
            ctx.config.paths.fulltext_dir,
            max_concurrency=ctx.config.profile.fulltext.concurrency,
        )

    for result in results:
        record_row = row_by_id.get(result.get("id", ""), {})
        record_id = record_row.get("record_id", "")
        status = result.get("status", "")
        inventory_rows.append(
            {
                "record_id": record_id,
                "openalex_id": record_row.get("openalex_id", ""),
                "doi": record_row.get("doi", ""),
                "title": record_row.get("title", ""),
                "track": record_row.get("track", ""),
                "best_pdf_url": record_row.get("best_pdf_url", ""),
                "download_attempted": "True",
                "download_status": status,
                "pdf_local_path": result.get("pdf_path") or "",
                "parse_status": status,
                "extraction_ready": str(status == "parsed"),
            }
        )
        pdf_rows.append(
            {
                "record_id": record_id,
                "best_pdf_url": record_row.get("best_pdf_url", ""),
                "status": status,
                "pdf_local_path": result.get("pdf_path") or "",
            }
        )
        parse_rows.append(
            {
                "record_id": record_id,
                "pdf_local_path": result.get("pdf_path") or "",
                "parse_status": status,
                "sections_detected": ",".join(sorted(result.get("framework_sections", {}).keys())),
            }
        )
        if record_id in screening_ft_by_id:
            screening_ft_by_id[record_id]["pdf_local_path"] = result.get("pdf_path") or ""
            screening_ft_by_id[record_id]["pdf_parse_status"] = status
            screening_ft_by_id[record_id]["fulltext_sections_seen"] = ",".join(sorted(result.get("framework_sections", {}).keys()))
            screening_ft_by_id[record_id]["equations_detected"] = "True" if "E" in result.get("framework_sections", {}) else "False"
            screening_ft_by_id[record_id]["figures_detected"] = "False"

    write_csv(ctx.config.paths.run_root / "fulltext_inventory.csv", inventory_rows, FULLTEXT_INVENTORY_COLUMNS)
    write_csv(ctx.config.paths.run_root / "pdf_download_log.csv", pdf_rows, PDF_DOWNLOAD_COLUMNS)
    write_csv(ctx.config.paths.run_root / "parse_log.csv", parse_rows, PARSE_LOG_COLUMNS)
    write_csv(ctx.config.paths.run_root / "screening_fulltext.csv", list(screening_ft_by_id.values()), SCREENING_FT_COLUMNS)


def _extract_summary(text: str, preferred_terms: Sequence[str]) -> str:
    for term in preferred_terms:
        snippet = snippet_for_term(text, term)
        if snippet:
            return snippet
    return first_sentence(text)


def stage_extract(ctx: PipelineContext) -> None:
    metadata_rows = read_csv(ctx.config.paths.run_root / "metadata.csv")
    parse_log = {row["record_id"]: row for row in read_csv(ctx.config.paths.run_root / "parse_log.csv")}
    extraction_rows: list[dict[str, Any]] = []

    for row in metadata_rows:
        if row.get("track") != "scholarly_core":
            continue
        text = " ".join((row.get("title", ""), row.get("machine_reason", ""))).strip()
        primary_label = row.get("label_primary_value", "")
        secondary_label = row.get("label_secondary_value", "")
        system_terms = _topic_extraction_hints(ctx, "system_terms", ["system", "service", "platform", "architecture"])
        disturbance_terms = _topic_extraction_hints(ctx, "disturbance_terms", ["fault", "failure", "outage", "disruption"])
        method_terms = _topic_extraction_hints(ctx, "method_terms", ["analysis", "simulation", "experiment", "evaluation"])
        workload_terms = _topic_extraction_hints(ctx, "workload_terms", ["workload", "trace", "request", "traffic"])
        validation_terms = _topic_extraction_hints(ctx, "validation_terms", ["simulation", "experiment", "case study", "benchmark", "trace"])
        locus_terms = _topic_extraction_hints(ctx, "disturbance_locus_terms", ["service", "node", "database", "network", "control plane"])
        scope_terms = _topic_extraction_hints(ctx, "disturbance_scope_terms", ["single component", "cluster", "multi-site", "system-wide"])
        extraction_rows.append(
            {
                "record_id": row["record_id"],
                "label_primary_dimension": row.get("label_primary_dimension", ""),
                "label_primary_value": primary_label,
                "label_secondary_dimension": row.get("label_secondary_dimension", ""),
                "label_secondary_value": secondary_label,
                "topic_labels_json": row.get("topic_labels_json", ""),
                "system_summary": _extract_summary(text, system_terms),
                "disturbance_summary": _extract_summary(text, disturbance_terms),
                "method_summary": _extract_summary(text, method_terms),
                "workload_summary": _extract_summary(text, workload_terms),
                "disturbance_type": ";".join(_term_hits(text, disturbance_terms)),
                "disturbance_locus": ";".join(_term_hits(text, locus_terms)),
                "disturbance_scope": ";".join(_term_hits(text, scope_terms)),
                "correlation_mode": "correlated" if "correlation" in text.lower() else "unspecified",
                "metrics_reported": ";".join(
                    _term_hits(
                        text,
                        unique_preserve_order(
                            [
                                *_topic_classifier_terms(ctx, "metric_terms", METRIC_TERMS),
                                *_topic_group_terms(ctx, "phenomenon", "construct", "outcome", "objective"),
                            ]
                        ),
                    )
                ),
                "equations_reported": str(parse_log.get(row["record_id"], {}).get("sections_detected", "").find("E") >= 0),
                "validation_mode": ";".join(_term_hits(text, validation_terms)),
                "artifact_links": row.get("best_pdf_url", ""),
                "key_findings": first_sentence(row.get("machine_reason", "")),
                "threats_to_validity": "Not automatically found; manual review may be required.",
                "auto_confidence": row.get("machine_confidence", ""),
                "evidence_spans": row.get("machine_reason", ""),
            }
        )
    write_csv(ctx.config.paths.run_root / "evidence_extraction.csv", extraction_rows, EVIDENCE_COLUMNS)


def stage_quality(ctx: PipelineContext) -> None:
    extraction_rows = read_csv(ctx.config.paths.run_root / "evidence_extraction.csv")
    quality_rows: list[dict[str, Any]] = []
    for row in extraction_rows:
        q1, q1_conf = _quality_score_from_hits(len(str(row.get("system_summary", "")).split()))
        q2, q2_conf = _quality_score_from_hits(len(str(row.get("method_summary", "")).split()))
        q3, q3_conf = _quality_score_from_hits(len(str(row.get("validation_mode", "")).split(";")))
        q4, q4_conf = _quality_score_from_hits(len(str(row.get("metrics_reported", "")).split(";")))
        q5, q5_conf = _quality_score_from_hits(1 if row.get("artifact_links") else 0)
        quality_rows.append(
            {
                "record_id": row["record_id"],
                "Q1_auto": q1,
                "Q1_confidence": f"{q1_conf:.2f}",
                "Q1_evidence": row.get("system_summary", ""),
                "Q2_auto": q2,
                "Q2_confidence": f"{q2_conf:.2f}",
                "Q2_evidence": row.get("method_summary", ""),
                "Q3_auto": q3,
                "Q3_confidence": f"{q3_conf:.2f}",
                "Q3_evidence": row.get("validation_mode", ""),
                "Q4_auto": q4,
                "Q4_confidence": f"{q4_conf:.2f}",
                "Q4_evidence": row.get("metrics_reported", ""),
                "Q5_auto": q5,
                "Q5_confidence": f"{q5_conf:.2f}",
                "Q5_evidence": row.get("artifact_links", ""),
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
                "resolved_Q1": q1,
                "resolved_Q2": q2,
                "resolved_Q3": q3,
                "resolved_Q4": q4,
                "resolved_Q5": q5,
                "notes": "",
            }
        )
    write_csv(ctx.config.paths.run_root / "quality_appraisal.csv", quality_rows, QUALITY_COLUMNS)


async def _fetch_gray_page(client: httpx.AsyncClient, url: str) -> tuple[int, str, str]:
    response = await client.get(url, follow_redirects=True, timeout=30.0)
    body = response.text[:200000]
    return response.status_code, response.url.__str__(), body


def _extract_html_title(body: str) -> str:
    match = re.search(r"<title>(.*?)</title>", body, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return re.sub(r"\s+", " ", match.group(1)).strip()


def _extract_meta_date(body: str) -> str:
    patterns = [
        r'property=["\']article:published_time["\']\s+content=["\']([^"\']+)["\']',
        r'name=["\']date["\']\s+content=["\']([^"\']+)["\']',
        r'"datePublished"\s*:\s*"([^"]+)"',
        r'"dateModified"\s*:\s*"([^"]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, body, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


async def stage_gray(ctx: PipelineContext) -> None:
    if not ctx.config.profile.gray.enabled:
        write_csv(ctx.config.paths.run_root / "gray_candidates.csv", [], GRAY_CANDIDATES_COLUMNS)
        write_csv(ctx.config.paths.run_root / "gray_appraisal_auto.csv", [], GRAY_CANDIDATES_COLUMNS)
        write_csv(ctx.config.paths.run_root / "gray_review_queue.csv", [], GRAY_CANDIDATES_COLUMNS)
        write_csv(ctx.config.paths.run_root / "gray_registry_resolution.csv", [], ["family_id", "domain", "status"])
        write_csv(ctx.config.paths.run_root / "gray_source_family_counts.csv", [], GRAY_COUNTS_COLUMNS)
        return

    families = ctx.gray_registry_payload.get("families", [])
    candidate_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []
    registry_rows: list[dict[str, Any]] = []
    count_rows: list[dict[str, Any]] = []
    async with httpx.AsyncClient(headers={"User-Agent": f"SciSieve/{__version__}"}) as client:
        for family in families:
            if not isinstance(family, dict):
                continue
            family_id = str(family.get("id") or "")
            family_candidates: list[dict[str, Any]] = []
            seeds = family.get("seed_urls", [])[: ctx.config.profile.gray.max_urls_per_family]
            for seed in seeds:
                if not isinstance(seed, dict):
                    continue
                url = str(seed.get("url") or "")
                try:
                    status_code, final_url, body = await _fetch_gray_page(client, url)
                except Exception:
                    registry_rows.append({"family_id": family_id, "domain": urlparse(url).netloc, "status": "fetch_failed"})
                    continue
                title = _extract_html_title(body) or url
                published_date = _extract_meta_date(body)
                lowered = body.lower()
                gray_signal_terms = unique_preserve_order(
                    [
                        *_topic_extraction_hints(ctx, "system_terms", ["system", "service", "platform"]),
                        *_topic_extraction_hints(ctx, "disturbance_terms", ["fault", "failure", "outage"]),
                        *_topic_extraction_hints(ctx, "method_terms", ["analysis", "simulation", "experiment"]),
                    ]
                )
                technical_specificity = sum(
                    1
                    for token in gray_signal_terms
                    if token in lowered
                )
                provenance_score = 2 if urlparse(final_url).netloc in family.get("domains", []) else 1
                authority = 2 if seed.get("organization") else 1
                date_score = 2 if published_date else 0
                marketing = any(token in lowered for token in ("contact sales", "pricing", "request demo"))
                include_recommendation = "include"
                if marketing:
                    include_recommendation = "reject"
                elif technical_specificity < 2 or not published_date:
                    include_recommendation = "borderline"
                gray_id = f"gray_{sha256_text(final_url)[:16]}"
                row = {
                    "gray_id": gray_id,
                    "family_id": family_id,
                    "organization": seed.get("organization", ""),
                    "domain": urlparse(final_url).netloc,
                    "url": final_url,
                    "title": title,
                    "artifact_type": seed.get("artifact_type", ""),
                    "published_date": published_date,
                    "last_reviewed_date": "",
                    "retrieval_date": ctx.config.run_timestamp_utc,
                    "content_hash": sha256_text(body[:50000]),
                    "archive_snapshot_url": "",
                    "query_family": seed.get("query_family", ""),
                    "technical_specificity_score": technical_specificity,
                    "provenance_score": provenance_score,
                    "vendor_bias_risk": 1 if "blog" in final_url else 0,
                    "aacods_authority": authority,
                    "aacods_accuracy": 2 if technical_specificity >= 2 else 1,
                    "aacods_coverage": 2 if len(body) > 5000 else 1,
                    "aacods_objectivity": 1 if "best practice" in lowered else 2,
                    "aacods_date": date_score,
                    "aacods_significance": 2 if technical_specificity >= 3 else 1,
                    "auto_include_recommendation": include_recommendation,
                    "manual_final_decision": "",
                }
                family_candidates.append(row)
                candidate_rows.append(row)
                if include_recommendation == "borderline":
                    review_rows.append(row)
                registry_rows.append({"family_id": family_id, "domain": urlparse(final_url).netloc, "status": f"http_{status_code}"})
            count_rows.append(
                {
                    "family_id": family_id,
                    "candidate_count": len(family_candidates),
                    "include_recommended": sum(1 for row in family_candidates if row["auto_include_recommendation"] == "include"),
                    "borderline": sum(1 for row in family_candidates if row["auto_include_recommendation"] == "borderline"),
                    "reject": sum(1 for row in family_candidates if row["auto_include_recommendation"] == "reject"),
                }
            )
    write_csv(ctx.config.paths.run_root / "gray_candidates.csv", candidate_rows, GRAY_CANDIDATES_COLUMNS)
    write_csv(ctx.config.paths.run_root / "gray_appraisal_auto.csv", candidate_rows, GRAY_CANDIDATES_COLUMNS)
    write_csv(ctx.config.paths.run_root / "gray_review_queue.csv", review_rows, GRAY_CANDIDATES_COLUMNS)
    write_csv(ctx.config.paths.run_root / "gray_registry_resolution.csv", registry_rows, ["family_id", "domain", "status"])
    write_csv(ctx.config.paths.run_root / "gray_source_family_counts.csv", count_rows, GRAY_COUNTS_COLUMNS)


def _write_false_negative_audit(ctx: PipelineContext) -> None:
    screening_rows = read_csv(ctx.config.paths.run_root / "screening_title_abstract.csv")
    positive_titles = [row.get("anchor_title", "") for row in _positive_anchor_rows(ctx)]
    scored_rows: list[dict[str, Any]] = []
    for row in screening_rows:
        if row.get("machine_decision") not in {"exclude", "needs_fulltext"}:
            continue
        similarity = max(
            (
                SequenceMatcher(None, normalize_title(row.get("title", "")), normalize_title(title)).ratio()
                for title in positive_titles
                if title
            ),
            default=0.0,
        )
        publication_year = row.get("year") or ""
        score = similarity * 0.5
        if publication_year and str(publication_year).isdigit():
            score += max(0.0, min(1.0, (int(publication_year) - 2010) / 16.0)) * 0.25
        if row.get("machine_decision") == "needs_fulltext":
            score += 0.25
        scored_rows.append(
            {
                "record_id": row.get("record_id", ""),
                "title": row.get("title", ""),
                "doi": row.get("doi", ""),
                "citation_count": 0,
                "publication_year": publication_year,
                "machine_decision": row.get("machine_decision", ""),
                "score": f"{score:.4f}",
                "reason": row.get("machine_reason", ""),
            }
        )
    scored_rows.sort(key=lambda item: float(item["score"]), reverse=True)
    write_csv(
        ctx.config.paths.run_root / "false_negative_audit_sample.csv",
        scored_rows[: ctx.config.profile.review.false_negative_audit_size],
        FALSE_NEGATIVE_COLUMNS,
    )


def _write_coverage_report(
    ctx: PipelineContext,
    coverage_rows: Sequence[Mapping[str, Any]],
    negative_rows: Sequence[Mapping[str, Any]],
    failure_rows: Sequence[Mapping[str, Any]],
) -> None:
    tier_a = [row for row in coverage_rows if row.get("tier") == "A"]
    tier_b = [row for row in coverage_rows if row.get("tier") == "B"]
    tier_a_ok = sum(1 for row in tier_a if row.get("retrieved") == "True" or row.get("diagnosis") == "not_indexed_in_primary_source")
    tier_b_ok = sum(1 for row in tier_b if row.get("retrieved") == "True" or row.get("diagnosis") == "not_indexed_in_primary_source")
    negative_failures = sum(1 for row in negative_rows if row.get("failed_precision_gate") == "True")
    write_markdown(
        ctx.config.paths.run_root / "coverage_report.md",
        "# Coverage Report\n\n"
        f"- Tier A discovered or diagnosed: {tier_a_ok}/{len(tier_a)}\n"
        f"- Tier B discovered or diagnosed: {tier_b_ok}/{len(tier_b)}\n"
        f"- Negative sentinels in core: {negative_failures}\n"
        f"- Coverage failures: {len(failure_rows)}\n",
    )


async def stage_anchor_check(ctx: PipelineContext) -> None:
    metadata_rows = read_csv(ctx.config.paths.run_root / "metadata.csv")
    screening_rows = {row["record_id"]: row for row in read_csv(ctx.config.paths.run_root / "screening_title_abstract.csv") if row.get("record_id")}
    normalized_rows = _normalized_rows(ctx)
    snowball_included_rows = read_csv(ctx.config.paths.run_root / "snowball_included.csv")
    snowball_excluded_rows = read_csv(ctx.config.paths.run_root / "snowball_excluded.csv")
    retrieved_rows = list(normalized_rows)
    retrieved_rows.extend(
        {
            "openalex_id": row.get("openalex_id", ""),
            "doi": row.get("doi", ""),
            "title": row.get("title", ""),
            "query_pack_ids": row.get("query_pack_ids", ""),
        }
        for row in [*snowball_included_rows, *snowball_excluded_rows]
    )
    retrieved_by_doi = {normalize_doi(row.get("doi")): row for row in retrieved_rows if normalize_doi(row.get("doi"))}
    retrieved_by_title = {normalize_title(row.get("title", "")): row for row in retrieved_rows if row.get("title")}
    metadata_by_doi = {normalize_doi(row.get("doi")): row for row in metadata_rows if normalize_doi(row.get("doi"))}
    metadata_by_title = {normalize_title(row.get("title", "")): row for row in metadata_rows if row.get("title")}

    coverage_rows: list[dict[str, Any]] = []
    negative_rows: list[dict[str, Any]] = []
    stage_loss_counter: Counter[str] = Counter()
    failure_rows: list[dict[str, Any]] = []

    async with OpenAlexClient(
        email=ctx.config.contact_email,
        requests_per_second=7.0,
        api_key=ctx.config.openalex_api_key,
    ) as openalex:
        for anchor in ctx.anchor_rows:
            anchor_id = anchor.get("anchor_id", "")
            polarity = anchor.get("polarity", "")
            anchor_doi = normalize_doi(anchor.get("doi"))
            anchor_title = normalize_title(anchor.get("anchor_title", ""))

            indexed_in_primary_source = "unknown"
            matched_retrieved = None
            if anchor_doi and anchor_doi in retrieved_by_doi:
                matched_retrieved = retrieved_by_doi[anchor_doi]
                indexed_in_primary_source = "True"
            elif anchor_title and anchor_title in retrieved_by_title:
                matched_retrieved = retrieved_by_title[anchor_title]
                indexed_in_primary_source = "True"
            elif anchor_doi:
                work = await openalex.lookup_work_by_doi(anchor_doi)
                indexed_in_primary_source = "True" if work else "False"
            else:
                indexed_in_primary_source = "False"

            matched_final = None
            if anchor_doi and anchor_doi in metadata_by_doi:
                matched_final = metadata_by_doi[anchor_doi]
            elif anchor_title and anchor_title in metadata_by_title:
                matched_final = metadata_by_title[anchor_title]

            retrieved = matched_retrieved is not None
            retrieved_pack_ids = matched_retrieved.get("query_pack_ids", "") if matched_retrieved else ""
            final_track = matched_final.get("track", "") if matched_final else ""
            matched_record_id = matched_final.get("record_id", "") if matched_final else ""
            lost_stage = ""
            diagnosis = ""
            action_required = "False"

            if indexed_in_primary_source != "True":
                lost_stage = "not_indexed"
                diagnosis = "not_indexed_in_primary_source"
            elif not retrieved:
                lost_stage = "not_retrieved"
                diagnosis = "not_retrieved"
                action_required = "True"
            elif matched_final:
                diagnosis = "discovered"
            else:
                record_id = _record_id_for_work(
                    str(matched_retrieved.get("openalex_id") or ""),
                    matched_retrieved.get("doi"),
                    str(matched_retrieved.get("title") or ""),
                )
                screening = screening_rows.get(record_id, {})
                decision = screening.get("machine_decision", "")
                if decision == "tertiary_background":
                    lost_stage = "tertiary_routed"
                    diagnosis = "tertiary_background"
                elif decision == "exclude":
                    lost_stage = "screen_ta_excluded"
                    diagnosis = screening.get("machine_reason", "") or "screen_ta_excluded"
                    action_required = "True"
                elif decision == "needs_fulltext":
                    lost_stage = "screen_ft_excluded"
                    diagnosis = "needs_fulltext_queue"
                    action_required = "True"
                else:
                    lost_stage = "dedup_removed"
                    diagnosis = "dedup_removed_or_unresolved"
                    action_required = "True"
            if lost_stage:
                stage_loss_counter[lost_stage] += 1

            row = {
                "anchor_id": anchor_id,
                "polarity": polarity,
                "tier": anchor.get("tier", ""),
                "expected_stage": anchor.get("expected_stage", ""),
                "indexed_in_primary_source": indexed_in_primary_source,
                "retrieved": str(retrieved),
                "retrieved_pack_ids": retrieved_pack_ids,
                "lost_stage": lost_stage,
                "final_track": final_track,
                "matched_record_id": matched_record_id,
                "diagnosis": diagnosis,
                "action_required": action_required,
            }
            if polarity == "positive":
                coverage_rows.append(row)
                if action_required == "True" and anchor.get("tier") == "A":
                    failure_rows.append(
                        {
                            "anchor_id": anchor_id,
                            "tier": anchor.get("tier", ""),
                            "diagnosis": diagnosis,
                            "action_required": action_required,
                        }
                    )
            else:
                failed_precision = "True" if matched_final and matched_final.get("track") == "scholarly_core" else "False"
                negative_rows.append(
                    {
                        "anchor_id": anchor_id,
                        "retrieved": str(retrieved),
                        "retrieved_pack_ids": retrieved_pack_ids,
                        "screen_ta_status": diagnosis,
                        "final_track": final_track,
                        "failed_precision_gate": failed_precision,
                    }
                )
                if failed_precision == "True":
                    failure_rows.append(
                        {
                            "anchor_id": anchor_id,
                            "tier": "negative",
                            "diagnosis": "negative_sentinel_in_core",
                            "action_required": "True",
                        }
                    )

    write_csv(ctx.config.paths.run_root / "coverage_anchor_results.csv", coverage_rows, COVERAGE_COLUMNS)
    write_csv(ctx.config.paths.run_root / "negative_sentinel_report.csv", negative_rows, NEGATIVE_SENTINEL_COLUMNS)
    write_csv(
        ctx.config.paths.run_root / "stage_loss_summary.csv",
        [{"lost_stage": stage, "count": count} for stage, count in sorted(stage_loss_counter.items())],
        STAGE_LOSS_COLUMNS,
    )
    write_csv(ctx.config.paths.run_root / "coverage_failures.csv", failure_rows, COVERAGE_FAILURE_COLUMNS)
    _write_false_negative_audit(ctx)
    _write_pack_reports(ctx)
    _write_coverage_report(ctx, coverage_rows, negative_rows, failure_rows)


def stage_audit(ctx: PipelineContext) -> None:
    coverage_failures = read_csv(ctx.config.paths.run_root / "coverage_failures.csv")
    borderline_rows = read_csv(ctx.config.paths.run_root / "review_queue_borderline.csv")
    negative_rows = read_csv(ctx.config.paths.run_root / "negative_sentinel_report.csv")
    quality_rows = read_csv(ctx.config.paths.run_root / "quality_appraisal.csv")

    audit_flags: list[dict[str, Any]] = []
    for row in coverage_failures:
        audit_flags.append(
            {
                "flag_type": "coverage_failure",
                "record_or_anchor_id": row.get("anchor_id", ""),
                "severity": "high",
                "details": row.get("diagnosis", ""),
            }
        )
    for row in negative_rows:
        if row.get("failed_precision_gate") == "True":
            audit_flags.append(
                {
                    "flag_type": "negative_sentinel_leakage",
                    "record_or_anchor_id": row.get("anchor_id", ""),
                    "severity": "critical",
                    "details": "Negative sentinel survived into scholarly_core",
                }
            )
    for row in borderline_rows:
        audit_flags.append(
            {
                "flag_type": "borderline_queue",
                "record_or_anchor_id": row.get("record_id", ""),
                "severity": "manual-review",
                "details": row.get("machine_reason", ""),
            }
        )
    for row in quality_rows:
        low_confidence = any(float(row.get(column, 0) or 0) < 0.6 for column in ("Q1_confidence", "Q2_confidence", "Q3_confidence", "Q4_confidence", "Q5_confidence"))
        if low_confidence:
            audit_flags.append(
                {
                    "flag_type": "low_quality_confidence",
                    "record_or_anchor_id": row.get("record_id", ""),
                    "severity": "manual-review",
                    "details": "At least one quality auto-score has low confidence.",
                }
            )
    flag_columns = ["flag_type", "record_or_anchor_id", "severity", "details"]
    write_csv(ctx.config.paths.run_root / "dataset_audit_flags.csv", audit_flags, flag_columns)
    write_markdown(
        ctx.config.paths.run_root / "dataset_audit_summary.txt",
        "Audit summary\n"
        f"- total flags: {len(audit_flags)}\n"
        f"- coverage failures: {sum(1 for row in audit_flags if row['flag_type'] == 'coverage_failure')}\n"
        f"- borderline queue items: {sum(1 for row in audit_flags if row['flag_type'] == 'borderline_queue')}\n",
    )
    write_markdown(
        ctx.config.paths.run_root / "dataset_audit_note.md",
        "# Dataset Audit Note\n\n"
        "This audit aggregates machine-detectable issues that still require explicit attention before a production release.\n",
    )


def _counts_for_manuscript(ctx: PipelineContext) -> dict[str, Any]:
    metadata_rows = read_csv(ctx.config.paths.run_root / "metadata.csv")
    quality_rows = read_csv(ctx.config.paths.run_root / "quality_appraisal.csv")
    extraction_rows = read_csv(ctx.config.paths.run_root / "evidence_extraction.csv")
    gray_rows = read_csv(ctx.config.paths.run_root / "gray_candidates.csv")

    primary_label_counts = Counter(
        row.get("label_primary_value", "")
        for row in metadata_rows
        if row.get("track") == "scholarly_core"
    )
    validation_counts = Counter()
    metric_counts = Counter()
    for row in extraction_rows:
        for value in str(row.get("validation_mode", "")).split(";"):
            if value:
                validation_counts[value] += 1
        for value in str(row.get("metrics_reported", "")).split(";"):
            if value:
                metric_counts[value] += 1
    return {
        "final_core_count": sum(1 for row in metadata_rows if row.get("track") == "scholarly_core"),
        "tertiary_count": sum(1 for row in metadata_rows if row.get("track") == "scholarly_tertiary"),
        "preprint_watchlist_count": sum(1 for row in metadata_rows if row.get("track") == "preprint_watchlist"),
        "gray_included_count": sum(1 for row in gray_rows if row.get("auto_include_recommendation") == "include"),
        "quality_rows": len(quality_rows),
        "counts_by_primary_label": dict(sorted(primary_label_counts.items())),
        "counts_by_metric": dict(sorted(metric_counts.items())),
        "counts_by_validation_type": dict(sorted(validation_counts.items())),
    }


def _write_method_reports(ctx: PipelineContext) -> None:
    query_text = (ctx.config.paths.run_root / "search_strings.txt").read_text(encoding="utf-8")
    dedup_summary = read_csv(ctx.config.paths.run_root / "dedup_summary.csv")
    prisma_rows = read_csv(ctx.config.paths.run_root / "prisma_counts.csv")
    write_markdown(
        ctx.config.paths.run_root / "prisma_s_appendix.md",
        "# PRISMA-S Appendix\n\n"
        f"Search date window: {ctx.config.profile.scholarly.start_date} to {ctx.config.resolved_end_date}\n\n"
        "## Query packs\n\n"
        f"```\n{query_text}\n```\n\n"
        "## Generated counts\n\n"
        + "\n".join(f"- {row['stage']}: {row['count']}" for row in prisma_rows),
    )
    write_markdown(
        ctx.config.paths.run_root / "press_peer_review.md",
        "# PRESS Peer Review Package\n\n"
        "## Pack definitions and exclusions\n\n"
        f"```\n{query_text}\n```\n\n"
        "## Dedup summary\n\n"
        f"```\n{json.dumps(dedup_summary, indent=2, ensure_ascii=False)}\n```\n",
    )
    seed_rows = read_csv(ctx.config.paths.run_root / "citation_seed_set.csv")
    round_rows = read_csv(ctx.config.paths.run_root / "snowball_rounds.csv")
    write_markdown(
        ctx.config.paths.run_root / "tarcis_appendix.md",
        "# TARCiS Appendix\n\n"
        f"- seed count: {len(seed_rows)}\n"
        f"- rounds executed: {len(round_rows)}\n"
        "- direction: backward + forward\n",
    )
    write_markdown(
        ctx.config.paths.run_root / "threats_to_validity.md",
        "# Threats To Validity\n\n"
        "- OpenAlex indexing and metadata completeness can affect retrieval.\n"
        "- Machine screening and extraction use deterministic heuristics and still require targeted human adjudication for borderline items.\n"
        "- Gray-literature automation is topic-bundle scoped and may miss relevant official pages that are absent from configured seeds.\n",
    )


def stage_release(ctx: PipelineContext) -> None:
    _assert_profile_guards(ctx)
    coverage_failures = read_csv(ctx.config.paths.run_root / "coverage_failures.csv")
    if coverage_failures and not ctx.config.profile.non_production:
        raise RuntimeError("Release blocked: coverage_failures.csv is non-empty.")
    non_production_note = "This package was generated from a debug/non-production profile.\n" if ctx.config.profile.non_production else ""

    if not _raw_pack_paths(ctx) and not ctx.config.profile.non_production:
        raise RuntimeError("Release blocked: frozen raw layer is missing.")

    metadata_rows = read_csv(ctx.config.paths.run_root / "metadata.csv")
    if not ctx.config.profile.non_production:
        missing_pack_provenance = [row["record_id"] for row in metadata_rows if not row.get("query_pack_ids")]
        if missing_pack_provenance:
            raise RuntimeError("Release blocked: retained records are missing query-pack provenance.")

    counts = _counts_for_manuscript(ctx)
    write_json(ctx.config.paths.run_root / "counts_for_manuscript.json", counts)
    write_markdown(
        ctx.config.paths.run_root / "manuscript_sync_report.md",
        "# Manuscript Sync Report\n\n"
        "All manuscript-facing counts in this package were generated directly from pipeline artifacts.\n\n"
        f"{non_production_note}",
    )
    _write_method_reports(ctx)

    release_dir = ctx.config.paths.release_dir
    if release_dir.exists():
        shutil.rmtree(release_dir)
    release_dir.mkdir(parents=True, exist_ok=True)
    ctx.config.paths.tables_dir.mkdir(parents=True, exist_ok=True)
    ctx.config.paths.figures_dir.mkdir(parents=True, exist_ok=True)

    write_markdown(
        release_dir / "README.md",
        f"# {ctx.config.app.project.release_title}\n\nProfile: {ctx.config.profile_name}\n\nRun ID: {ctx.config.run_id}\n\n{non_production_note}",
    )
    write_markdown(
        release_dir / "LICENSE",
        "MIT License\n\nCopyright (c) SciSieve Team\n\nPermission is hereby granted, free of charge, "
        "to any person obtaining a copy of this software and associated documentation files (the \"Software\"), to deal in the Software without restriction...\n",
    )
    authors = ctx.config.app.project.citation_authors or []
    citation_lines = ["cff-version: 1.2.0", f'title: "{ctx.config.app.project.release_title}"', "type: software", "authors:"]
    if authors:
        for author in authors:
            citation_lines.append(f"  - name: {author.name}")
            if author.orcid:
                citation_lines.append(f"    orcid: {author.orcid}")
    else:
        citation_lines.append("  - name: SciSieve Team")
    citation_lines.append(f"version: {__version__}")
    write_markdown(release_dir / "CITATION.cff", "\n".join(citation_lines))
    write_markdown(
        release_dir / "Dockerfile",
        "FROM python:3.11-slim\nWORKDIR /app\nCOPY . /app\nRUN pip install -r requirements.lock\nCMD [\"python\", \"-m\", \"scisieve\", \"run\", \"--config\", \"scisieve.yaml\", \"--profile\", \"debug\"]\n",
    )
    write_markdown(
        release_dir / "Makefile",
        "install:\n\tpython -m pip install -r requirements.lock\n\nrun-debug:\n\tpython -m scisieve run --config scisieve.yaml --profile debug\n",
    )
    write_markdown(
        release_dir / "reproduce.sh",
        "#!/usr/bin/env bash\nset -euo pipefail\npython -m pip install -r requirements.lock\npython -m scisieve run --config scisieve.yaml --profile debug\n",
    )
    shutil.copy2(ctx.config.paths.query_packs_path, release_dir / "query_packs.yaml")
    shutil.copy2(ctx.config.paths.gray_registry_path, release_dir / "gray_registry.yaml")
    shutil.copy2(ctx.config.config_path, release_dir / "scisieve.yaml")
    shutil.copy2(ctx.config.repo_root / "requirements.txt", release_dir / "requirements.lock")

    required_files = [
        "run_manifest.json",
        "metadata.csv",
        "prisma_counts.csv",
        "dedup_summary.csv",
        "coverage_anchor_results.csv",
        "snowball_rounds.csv",
        "screening_title_abstract.csv",
        "screening_fulltext.csv",
        "quality_appraisal.csv",
        "evidence_extraction.csv",
        "preprint_resolution.csv",
        "gray_candidates.csv",
        "gray_appraisal_auto.csv",
        "search_strings.txt",
        "coverage_report.md",
        "press_peer_review.md",
        "prisma_s_appendix.md",
        "tarcis_appendix.md",
        "threats_to_validity.md",
        "counts_for_manuscript.json",
        "manuscript_sync_report.md",
    ]
    release_manifest_rows: list[dict[str, Any]] = []
    for filename in required_files:
        source = ctx.config.paths.run_root / filename
        if not source.exists():
            continue
        destination = release_dir / filename
        shutil.copy2(source, destination)
        release_manifest_rows.append({"path": filename, "sha256": sha256_file(destination), "size_bytes": destination.stat().st_size})

    write_json(
        release_dir / "release_manifest.json",
        {
            "run_id": ctx.config.run_id,
            "profile": ctx.config.profile_name,
            "tool_version": __version__,
            "files": release_manifest_rows,
        },
    )


STAGE_SEQUENCE: list[str] = [
    "freeze_scholarly",
    "retrieve_scholarly",
    "normalize",
    "dedup",
    "screen_ta",
    "snowball",
    "fulltext",
    "extract",
    "quality",
    "gray",
    "anchor_check",
    "audit",
    "release",
]


async def _run_named_stage(ctx: PipelineContext, stage_name: str) -> None:
    if stage_name == "freeze_scholarly":
        await stage_freeze_scholarly(ctx)
        return
    if stage_name == "retrieve_scholarly":
        stage_retrieve_scholarly(ctx)
        return
    if stage_name == "normalize":
        stage_normalize(ctx)
        return
    if stage_name == "dedup":
        await stage_dedup(ctx)
        return
    if stage_name == "screen_ta":
        stage_screen_ta(ctx)
        return
    if stage_name == "snowball":
        await stage_snowball(ctx)
        return
    if stage_name == "fulltext":
        await stage_fulltext(ctx)
        return
    if stage_name == "extract":
        stage_extract(ctx)
        return
    if stage_name == "quality":
        stage_quality(ctx)
        return
    if stage_name == "gray":
        await stage_gray(ctx)
        return
    if stage_name == "anchor_check":
        await stage_anchor_check(ctx)
        return
    if stage_name == "audit":
        stage_audit(ctx)
        return
    if stage_name == "release":
        stage_release(ctx)
        return
    raise RuntimeError(f"Unknown stage: {stage_name}")


async def run_pipeline(ctx: PipelineContext) -> None:
    _assert_profile_guards(ctx)
    _write_run_manifest(ctx)
    resume_state = _load_resume_state(ctx)
    start_index = 0
    if resume_state.get("status") == "paused_budget":
        paused_stage = str(resume_state.get("stage") or "")
        if paused_stage in STAGE_SEQUENCE:
            start_index = STAGE_SEQUENCE.index(paused_stage)
    for stage_name in STAGE_SEQUENCE[start_index:]:
        _write_resume_state(ctx, status="running", stage=stage_name)
        try:
            await _run_named_stage(ctx, stage_name)
        except OpenAlexBudgetExceeded as exc:
            _write_resume_state(
                ctx,
                status="paused_budget",
                stage=stage_name,
                detail=str(exc),
                resume_after_utc=exc.reset_at_utc,
            )
            return
    _write_resume_state(ctx, status="completed", stage="release")


async def execute_async(command: str, ctx: PipelineContext, *, target: str = "") -> None:
    if command == "run":
        await run_pipeline(ctx)
        return
    stage_name = ""
    if command == "freeze" and target == "scholarly":
        stage_name = "freeze_scholarly"
    elif command == "dedup":
        stage_name = "dedup"
    elif command == "snowball":
        stage_name = "snowball"
    elif command == "fulltext":
        stage_name = "fulltext"
    elif command == "gray":
        stage_name = "gray"
    elif command == "anchor-check":
        stage_name = "anchor_check"
    if stage_name:
        _write_resume_state(ctx, status="running", stage=stage_name)
        try:
            await _run_named_stage(ctx, stage_name)
        except OpenAlexBudgetExceeded as exc:
            _write_resume_state(
                ctx,
                status="paused_budget",
                stage=stage_name,
                detail=str(exc),
                resume_after_utc=exc.reset_at_utc,
            )
        else:
            _write_resume_state(ctx, status="completed", stage=stage_name)
        return
    raise RuntimeError(f"Unsupported async command: {command} {target}".strip())


def execute(command: str, ctx: PipelineContext, *, target: str = "") -> None:
    if command in {"run", "freeze", "dedup", "snowball", "fulltext", "gray", "anchor-check"}:
        asyncio.run(execute_async(command, ctx, target=target))
        return
    if command == "retrieve":
        stage_retrieve_scholarly(ctx)
        return
    if command == "normalize":
        stage_normalize(ctx)
        return
    if command == "screen-ta":
        stage_screen_ta(ctx)
        return
    if command == "extract":
        stage_extract(ctx)
        return
    if command == "quality":
        stage_quality(ctx)
        return
    if command == "release":
        stage_release(ctx)
        return
    if command == "audit":
        stage_audit(ctx)
        return
    raise RuntimeError(f"Unsupported command: {command}")

