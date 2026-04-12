from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

from models import reconstruct_abstract


TERM_GROUPS: dict[str, list[str]] = {
    "construct": ["resilien*", "dependab*", "fault toleran*", "self-heal*"],
    "context": [
        "cloud computing",
        "cloud infrastructure",
        "cloud environment",
        "cloud system",
        "cloud service",
        "cloud platform",
        "cloud-based software service",
        "cloud-based software services",
        "cloud-native",
        "openstack",
        "eucalyptus",
        "iaas",
        "it service",
        "it services",
        "microservice*",
        "kubernetes",
        "k8s",
        "serverless",
        "multi-cloud",
        "hybrid cloud",
        "data center",
        "container orchestration",
        "containerized service",
        "cloud-fog",
        "cloud-edge",
    ],
    "model_paradigm": [
        "modeling",
        "modelling",
        "model-based",
        "simulation",
        "simulator",
        "digital twin",
        "resilience assessment",
        "profiling",
        "testing framework",
        "fault injection",
        "fault diagnosis",
        "failure prediction",
        "proactive actions",
        "chaos engineering",
        "formal verification",
        "model checking",
        "machine learning",
        "deep learning",
        "reinforcement learning",
        "AIOps",
    ],
}

NEGATIVE_EXCLUSION_TERMS: list[str] = [
    "cloud model",
    "container transportation",
    "container terminal",
    "port-hinterland",
    "urban flood",
    "stormwater",
    "vehicle-road-cloud",
    "blockchain",
    "wind turbine",
    "internet of medical things",
    "healthcare things",
    "logistics",
]

WILDCARD_EXPANSIONS: dict[str, list[str]] = {
    "resilien*": ["resilience", "resilient", "resiliency"],
    "dependab*": ["dependability", "dependable"],
    "fault toleran*": ["fault tolerance", "fault tolerant", "fault-tolerant"],
    "self-heal*": ["self-healing", "self-heal"],
    "microservice*": ["microservice", "microservices", "micro-service", "micro-services"],
}

CANONICAL_SEARCH_STRING = (
    '(resilien* OR dependab* OR "fault toleran*" OR "self-heal*") AND '
    '("cloud computing" OR "cloud infrastructure" OR "cloud environment" OR '
    '"cloud system" OR "cloud service" OR "cloud platform" OR "cloud-native" OR '
    'microservice* OR kubernetes OR k8s OR serverless OR "multi-cloud" OR '
    '"hybrid cloud" OR "data center" OR "container orchestration" OR '
    '"containerized service" OR "cloud-fog" OR "cloud-edge") AND '
    '(modeling OR modelling OR "model-based" OR simulation OR "digital twin" OR '
    '"fault injection" OR "chaos engineering" OR "formal verification" OR '
    '"model checking" OR "machine learning" OR "deep learning" OR '
    '"reinforcement learning" OR AIOps)'
)


def _normalize_for_match(text: str) -> str:
    lowered = text.lower()
    lowered = lowered.replace("ai ops", "aiops")
    lowered = re.sub(r"[\u2010-\u2015]", "-", lowered)
    return lowered


def _term_to_regex_pattern(term: str, *, allow_suffix_on_last_token: bool = False) -> str:
    cleaned = term.strip().lower()
    tokens = [token for token in re.split(r"[\s-]+", cleaned) if token]
    token_patterns: list[str] = []
    for index, token in enumerate(tokens):
        is_last_token = index == len(tokens) - 1
        if token.endswith("*") and len(token) > 1:
            token_patterns.append(rf"{re.escape(token[:-1])}\w*")
            continue
        if allow_suffix_on_last_token and is_last_token and token.isalpha():
            token_patterns.append(rf"{re.escape(token)}\w*")
            continue
        token_patterns.append(re.escape(token))

    if not token_patterns:
        return r"$^"
    if len(token_patterns) == 1:
        return rf"\b{token_patterns[0]}\b"
    return rf"\b{r'(?:[\s-]+)'.join(token_patterns)}\b"


def compile_terms_to_regex(terms: Sequence[str]) -> list[re.Pattern[str]]:
    return [re.compile(_term_to_regex_pattern(term), flags=re.IGNORECASE) for term in terms]


def compile_negative_regex(terms: Sequence[str] = NEGATIVE_EXCLUSION_TERMS) -> list[re.Pattern[str]]:
    return [
        re.compile(_term_to_regex_pattern(term, allow_suffix_on_last_token=True), flags=re.IGNORECASE)
        for term in terms
    ]


def compile_group_regex(term_groups: Mapping[str, Sequence[str]] = TERM_GROUPS) -> dict[str, list[re.Pattern[str]]]:
    return {group_name: compile_terms_to_regex(terms) for group_name, terms in term_groups.items()}


def has_negative_exclusion(
    text: str,
    *,
    compiled_exclusions: Sequence[re.Pattern[str]] | None = None,
) -> bool:
    normalized_text = _normalize_for_match(text)
    exclusions = compiled_exclusions if compiled_exclusions is not None else compile_negative_regex()
    return any(pattern.search(normalized_text) for pattern in exclusions)


def evaluate_protocol(
    text: str,
    *,
    compiled_groups: Mapping[str, Sequence[re.Pattern[str]]] | None = None,
    compiled_exclusions: Sequence[re.Pattern[str]] | None = None,
) -> tuple[bool, str | None]:
    if not text.strip():
        return False, "empty_screening_text"

    normalized_text = _normalize_for_match(text)
    if has_negative_exclusion(normalized_text, compiled_exclusions=compiled_exclusions):
        return False, "negative_domain_exclusion"

    compiled = compiled_groups if compiled_groups is not None else compile_group_regex()
    for group_patterns in compiled.values():
        if not any(pattern.search(normalized_text) for pattern in group_patterns):
            return False, "failed_protocol_match"
    return True, None


def matches_protocol(
    text: str,
    *,
    compiled_groups: Mapping[str, Sequence[re.Pattern[str]]] | None = None,
    compiled_exclusions: Sequence[re.Pattern[str]] | None = None,
) -> bool:
    matched, _reason = evaluate_protocol(
        text,
        compiled_groups=compiled_groups,
        compiled_exclusions=compiled_exclusions,
    )
    return matched


def build_screening_text(work: Mapping[str, Any]) -> str:
    title = str(work.get("title") or "")
    abstract = reconstruct_abstract(work.get("abstract_inverted_index"))
    return " ".join(part for part in (title, abstract) if part).strip()


def build_evidence_text(work: Mapping[str, Any]) -> str:
    return build_screening_text(work)


def matches_work(
    work: Mapping[str, Any],
    *,
    compiled_groups: Mapping[str, Sequence[re.Pattern[str]]] | None = None,
    compiled_exclusions: Sequence[re.Pattern[str]] | None = None,
) -> bool:
    return matches_protocol(
        build_screening_text(work),
        compiled_groups=compiled_groups,
        compiled_exclusions=compiled_exclusions,
    )


def expand_terms_for_openalex(
    term_groups: Mapping[str, Sequence[str]] = TERM_GROUPS,
) -> dict[str, list[str]]:
    expanded: dict[str, list[str]] = {}
    for group_name, terms in term_groups.items():
        tokens: list[str] = []
        for term in terms:
            mapped = WILDCARD_EXPANSIONS.get(term)
            if mapped:
                tokens.extend(mapped)
                continue
            tokens.append(term)
        expanded[group_name] = list(dict.fromkeys(token.strip() for token in tokens if token.strip()))
    return expanded


def _quote_term(term: str) -> str:
    return f'"{term}"' if (" " in term or "-" in term) else term


def build_openalex_query_from_expanded(expanded_groups: Mapping[str, Sequence[str]]) -> str:
    group_queries: list[str] = []
    for group_name in ("construct", "context", "model_paradigm"):
        terms = expanded_groups.get(group_name, [])
        if not terms:
            continue
        group_queries.append(f"({' OR '.join(_quote_term(term) for term in terms)})")
    return " AND ".join(group_queries)


def format_search_strings_report(
    *,
    canonical_search_string: str,
    expanded_groups: Mapping[str, Sequence[str]],
    negative_exclusions: Sequence[str],
    openalex_query: str,
    start_date: str,
    end_date: str,
    run_timestamp_utc: str,
) -> str:
    lines: list[str] = []
    lines.append("CANONICAL_SEARCH_STRING")
    lines.append(canonical_search_string)
    lines.append("")
    lines.append("EXPANDED_TERMS_FOR_OPENALEX")
    for group_name in ("construct", "context", "model_paradigm"):
        terms = ", ".join(expanded_groups.get(group_name, []))
        lines.append(f"{group_name}: {terms}")
    lines.append("")
    lines.append("NEGATIVE_DOMAIN_EXCLUSIONS")
    lines.extend(negative_exclusions)
    lines.append("")
    lines.append("SCREENING_SOURCE")
    lines.append("title+abstract")
    lines.append("")
    lines.append("OPENALEX_SEARCH_QUERY")
    lines.append(openalex_query)
    lines.append("")
    lines.append(f"START_DATE: {start_date}")
    lines.append(f"END_DATE: {end_date}")
    lines.append(f"RUN_TIMESTAMP_UTC: {run_timestamp_utc}")
    lines.append("")
    return "\n".join(lines)
