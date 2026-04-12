from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pydantic import BaseModel, Field


PROFILE_ALIASES = {
    "csur": "production",
}


class CitationAuthor(BaseModel):
    name: str
    orcid: str = ""


class ProjectConfig(BaseModel):
    release_title: str = "SciSieve Replication Package"
    citation_authors: list[CitationAuthor] = Field(default_factory=list)
    license_spdx: str = "MIT"


class PathsConfig(BaseModel):
    working_dir: str = ".scisieve_runs"
    query_packs: str = "topics/cloud_resilience_dependability/query_packs.yaml"
    gray_registry: str = "topics/cloud_resilience_dependability/gray_registry.yaml"
    topic_profile: str = "topics/cloud_resilience_dependability/topic_profile.yaml"
    anchor_benchmark: str
    baseline_metadata: str | None = None


class ScholarlyProfile(BaseModel):
    start_date: str = "2010-01-01"
    end_date: str = ""
    per_page: int = 200
    max_records_per_pack: int | None = None
    max_preprints_per_pack: int | None = None
    resolve_preprints: bool = True
    resolve_preprints_threshold: float = 0.92


class SnowballProfile(BaseModel):
    enabled: bool = True
    max_rounds: int = 5
    min_new_core_per_round: int = 3
    backward_refs_per_seed: int = 250
    forward_max_per_seed: int = 300
    max_seeds: int = 25


class FulltextProfile(BaseModel):
    enabled: bool = False
    limit: int = 25
    requests_per_second: float = 1.0
    concurrency: int = 3


class GrayProfile(BaseModel):
    enabled: bool = True
    max_urls_per_family: int = 20


class ReviewProfile(BaseModel):
    double_screen_fraction: float = 0.2
    double_screen_seed: int = 42
    false_negative_audit_size: int = 40


class ProfileConfig(BaseModel):
    non_production: bool = False
    input_mode: str = "frozen_api_export"
    snapshot_id: str = "openalex_live_freeze"
    scholarly: ScholarlyProfile
    snowball: SnowballProfile = Field(default_factory=SnowballProfile)
    fulltext: FulltextProfile = Field(default_factory=FulltextProfile)
    gray: GrayProfile = Field(default_factory=GrayProfile)
    review: ReviewProfile = Field(default_factory=ReviewProfile)


class AppConfig(BaseModel):
    version: int = 1
    topic: str
    description: str = ""
    contact_email: str
    project: ProjectConfig = Field(default_factory=ProjectConfig)
    paths: PathsConfig
    profiles: dict[str, ProfileConfig]


@dataclass(frozen=True)
class ResolvedPaths:
    repo_root: Path
    config_path: Path
    run_root: Path
    raw_dir: Path
    raw_scholarly_dir: Path
    raw_gray_dir: Path
    normalized_dir: Path
    reports_dir: Path
    fulltext_dir: Path
    release_dir: Path
    tables_dir: Path
    figures_dir: Path
    query_packs_path: Path
    gray_registry_path: Path
    topic_profile_path: Path
    anchor_benchmark_path: Path
    baseline_metadata_path: Path | None


@dataclass(frozen=True)
class ResolvedConfig:
    app: AppConfig
    profile_name: str
    profile: ProfileConfig
    config_path: Path
    repo_root: Path
    paths: ResolvedPaths
    contact_email: str
    resolved_end_date: str
    run_timestamp_utc: str
    run_id: str


def _resolve_path(base_dir: Path, raw_value: str | None) -> Path | None:
    if not raw_value:
        return None
    path = Path(raw_value)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def load_resolved_config(
    *,
    config_path: str,
    profile_name: str,
    email_override: str = "",
    run_root_override: str = "",
) -> ResolvedConfig:
    config_file = Path(config_path).resolve()
    repo_root = config_file.parent
    payload = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    app = AppConfig.model_validate(payload)
    profile_name = PROFILE_ALIASES.get(profile_name, profile_name)
    if profile_name not in app.profiles:
        known = ", ".join(sorted(app.profiles))
        raise ValueError(f"Unknown profile '{profile_name}'. Known profiles: {known}")
    profile = app.profiles[profile_name]
    resolved_end_date = profile.scholarly.end_date or datetime.now(tz=timezone.utc).date().isoformat()
    run_timestamp_utc = datetime.now(tz=timezone.utc).isoformat()
    run_id = f"{profile_name}-{datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    default_run_root = (repo_root / app.paths.working_dir / profile_name).resolve()
    run_root = Path(run_root_override).resolve() if run_root_override else default_run_root
    paths = ResolvedPaths(
        repo_root=repo_root,
        config_path=config_file,
        run_root=run_root,
        raw_dir=run_root / "raw",
        raw_scholarly_dir=run_root / "raw" / "scholarly",
        raw_gray_dir=run_root / "raw" / "gray",
        normalized_dir=run_root / "normalized",
        reports_dir=run_root / "reports",
        fulltext_dir=run_root / "fulltext",
        release_dir=run_root / "release_package",
        tables_dir=run_root / "release_package" / "tables",
        figures_dir=run_root / "release_package" / "figures",
        query_packs_path=_resolve_path(repo_root, app.paths.query_packs)
        or repo_root / "topics" / "cloud_resilience_dependability" / "query_packs.yaml",
        gray_registry_path=_resolve_path(repo_root, app.paths.gray_registry)
        or repo_root / "topics" / "cloud_resilience_dependability" / "gray_registry.yaml",
        topic_profile_path=_resolve_path(repo_root, app.paths.topic_profile)
        or repo_root / "topics" / "cloud_resilience_dependability" / "topic_profile.yaml",
        anchor_benchmark_path=_resolve_path(repo_root, app.paths.anchor_benchmark) or repo_root / "anchors.csv",
        baseline_metadata_path=_resolve_path(repo_root, app.paths.baseline_metadata),
    )
    return ResolvedConfig(
        app=app,
        profile_name=profile_name,
        profile=profile,
        config_path=config_file,
        repo_root=repo_root,
        paths=paths,
        contact_email=email_override or app.contact_email,
        resolved_end_date=resolved_end_date,
        run_timestamp_utc=run_timestamp_utc,
        run_id=run_id,
    )

