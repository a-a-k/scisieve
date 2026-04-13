from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field


class ResearchWork(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    source: str
    openalex_id: str | None = None
    title: str
    doi: str | None = None
    work_type: str = Field(alias="type")
    publication_date: str | None = None
    publication_year: int | None = None
    track: str = "core"
    archival_status: str = "archival"
    is_in_core_corpus: bool = True
    is_watchlist_candidate: bool = False
    watchlist_tag: str | None = None
    replaced_by_doi: str | None = None
    matched_archival_id: str | None = None
    matched_archival_doi: str | None = None
    resolved_from_preprint: bool = False
    label_primary_dimension: str | None = None
    label_primary_value: str | None = None
    label_secondary_dimension: str | None = None
    label_secondary_value: str | None = None
    topic_labels_json: str | None = None
    cited_by_count: int | None = None
    best_pdf_url: str | None = None
    abstract: str | None = None


def normalize_doi(raw_doi: str | None) -> str | None:
    if not raw_doi:
        return None
    doi = raw_doi.strip()
    doi = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", doi, flags=re.IGNORECASE)
    return doi.lower() if doi else None


def reconstruct_abstract(abstract_inverted_index: Mapping[str, Any] | None) -> str:
    if not abstract_inverted_index:
        return ""
    positions: list[tuple[int, str]] = []
    for token, index_positions in abstract_inverted_index.items():
        if not isinstance(index_positions, list):
            continue
        for pos in index_positions:
            if isinstance(pos, int):
                positions.append((pos, token))
    positions.sort(key=lambda item: item[0])
    return " ".join(token for _, token in positions)


def _normalize_classifier_text(text: str) -> str:
    lowered = text.lower()
    lowered = lowered.replace("ai ops", "aiops")
    lowered = re.sub(r"[\u2010-\u2015]", "-", lowered)
    return lowered


def term_in_text(term: str, text: str) -> bool:
    normalized_text = _normalize_classifier_text(text)
    tokens = [token for token in re.split(r"[\s-]+", term.lower()) if token]
    if not tokens:
        return False
    separator_pattern = r"(?:[\s-]+)"
    token_pattern = separator_pattern.join(re.escape(token) for token in tokens)
    pattern = rf"\b{token_pattern}\b"
    return re.search(pattern, normalized_text) is not None


def infer_dimension_label(text: str, dimension: Mapping[str, Any]) -> str:
    lowered = _normalize_classifier_text(text)
    default = str(dimension.get("default") or "Unknown")
    labels = dimension.get("labels", [])
    if not isinstance(labels, (list, tuple)):
        return default

    for label in labels:
        if not isinstance(label, dict):
            continue
        value = str(label.get("value") or "").strip()
        if not value:
            continue
        any_of = [str(term).strip() for term in label.get("any_of", []) if str(term).strip()]
        all_of = [str(term).strip() for term in label.get("all_of", []) if str(term).strip()]
        none_of = [str(term).strip() for term in label.get("none_of", []) if str(term).strip()]

        if any_of and not any(term_in_text(term, lowered) for term in any_of):
            continue
        if all_of and not all(term_in_text(term, lowered) for term in all_of):
            continue
        if none_of and any(term_in_text(term, lowered) for term in none_of):
            continue
        return value
    return default


def infer_topic_labels(text: str, dimensions: Sequence[Mapping[str, Any]] | None = None) -> dict[str, str]:
    labels: dict[str, str] = {}
    for dimension in dimensions or []:
        if not isinstance(dimension, dict):
            continue
        name = str(dimension.get("name") or "").strip()
        if not name:
            continue
        labels[name] = infer_dimension_label(text, dimension)
    return labels
