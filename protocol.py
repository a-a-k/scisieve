from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

from models import reconstruct_abstract


TERM_GROUPS: dict[str, list[str]] = {}

NEGATIVE_EXCLUSION_TERMS: list[str] = []

WILDCARD_EXPANSIONS: dict[str, list[str]] = {
    "resilien*": ["resilience", "resilient", "resiliency"],
    "dependab*": ["dependability", "dependable"],
    "fault toleran*": ["fault tolerance", "fault tolerant", "fault-tolerant"],
    "self-heal*": ["self-healing", "self-heal"],
    "microservice*": ["microservice", "microservices", "micro-service", "micro-services"],
}

CANONICAL_SEARCH_STRING = ""


def screening_term_groups(topic_profile: Mapping[str, Any] | None = None) -> dict[str, list[str]]:
    if topic_profile is None:
        return {group_name: list(terms) for group_name, terms in TERM_GROUPS.items()}
    screening_payload = topic_profile.get("screening", {})
    raw_groups = screening_payload.get("term_groups", {})
    if not isinstance(raw_groups, dict):
        return {group_name: list(terms) for group_name, terms in TERM_GROUPS.items()}
    groups: dict[str, list[str]] = {}
    for group_name, values in raw_groups.items():
        if not isinstance(values, (list, tuple)):
            continue
        cleaned = [str(value).strip() for value in values if str(value).strip()]
        if cleaned:
            groups[str(group_name)] = cleaned
    return groups or {group_name: list(terms) for group_name, terms in TERM_GROUPS.items()}


def negative_exclusion_terms(topic_profile: Mapping[str, Any] | None = None) -> list[str]:
    if topic_profile is None:
        return list(NEGATIVE_EXCLUSION_TERMS)
    screening_payload = topic_profile.get("screening", {})
    values = screening_payload.get("negative_exclusions", [])
    if not isinstance(values, (list, tuple)):
        return list(NEGATIVE_EXCLUSION_TERMS)
    cleaned = [str(value).strip() for value in values if str(value).strip()]
    return cleaned or list(NEGATIVE_EXCLUSION_TERMS)


def flatten_term_groups(term_groups: Mapping[str, Sequence[str]]) -> list[str]:
    flattened: list[str] = []
    for values in term_groups.values():
        flattened.extend(str(value).strip() for value in values if str(value).strip())
    return flattened


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
    separator_pattern = r"(?:[\s-]+)"
    joined_pattern = separator_pattern.join(token_patterns)
    return rf"\b{joined_pattern}\b"


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
    if not compiled:
        return False, "no_term_groups_configured"
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
    for group_name, terms in expanded_groups.items():
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
    for group_name, values in expanded_groups.items():
        terms = ", ".join(values)
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
