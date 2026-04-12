from __future__ import annotations

import re
from enum import Enum
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field


class ResilienceParadigm(str, Enum):
    UNKNOWN = "Unknown"
    ANALYTICS = "Analytics"
    SIMULATION = "Simulation"
    CHAOS = "Chaos"
    ML = "ML"
    FORMAL = "Formal"


class CloudContext(str, Enum):
    UNKNOWN = "Unknown"
    K8S = "K8s"
    EDGE = "Edge"
    SERVERLESS = "Serverless"
    MULTI_CLOUD = "MultiCloud"
    HYBRID_CLOUD = "HybridCloud"
    GENERAL_CLOUD = "GeneralCloud"


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
    resilience_paradigm: ResilienceParadigm
    cloud_context: CloudContext
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
    pattern = rf"\b{r'(?:[\s-]+)'.join(re.escape(token) for token in tokens)}\b"
    return re.search(pattern, normalized_text) is not None


def infer_resilience_paradigm(text: str) -> ResilienceParadigm:
    lowered = _normalize_classifier_text(text)
    if any(term_in_text(token, lowered) for token in ("formal verification", "model checking", "theorem proving")):
        return ResilienceParadigm.FORMAL
    if any(term_in_text(token, lowered) for token in ("chaos engineering", "fault injection", "chaos experiment")):
        return ResilienceParadigm.CHAOS
    if any(
        term_in_text(token, lowered)
        for token in ("machine learning", "deep learning", "aiops", "reinforcement learning")
    ):
        return ResilienceParadigm.ML
    if any(term_in_text(token, lowered) for token in ("simulation", "simulator", "digital twin")):
        return ResilienceParadigm.SIMULATION
    if any(
        term_in_text(token, lowered)
        for token in (
            "modeling",
            "modelling",
            "model-based",
            "analytical model",
            "stochastic model",
            "reliability model",
            "dependability model",
        )
    ):
        return ResilienceParadigm.ANALYTICS
    return ResilienceParadigm.UNKNOWN


def infer_cloud_context(text: str) -> CloudContext:
    lowered = _normalize_classifier_text(text)
    if any(term_in_text(token, lowered) for token in ("kubernetes", "k8s")):
        return CloudContext.K8S
    if any(term_in_text(token, lowered) for token in ("edge computing", "edge cloud", "cloud-edge", "fog computing", "cloud-fog")):
        return CloudContext.EDGE
    if any(term_in_text(token, lowered) for token in ("serverless", "faas")):
        return CloudContext.SERVERLESS
    if any(term_in_text(token, lowered) for token in ("multi-cloud", "multicloud")):
        return CloudContext.MULTI_CLOUD
    if term_in_text("hybrid cloud", lowered):
        return CloudContext.HYBRID_CLOUD
    if any(
        term_in_text(token, lowered)
        for token in (
            "cloud computing",
            "cloud infrastructure",
            "cloud environment",
            "cloud system",
            "cloud service",
            "cloud platform",
            "data center",
            "virtualized cloud",
            "iaas",
            "paas",
            "saas",
        )
    ):
        return CloudContext.GENERAL_CLOUD
    return CloudContext.UNKNOWN
