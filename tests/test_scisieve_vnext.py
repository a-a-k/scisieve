from __future__ import annotations

import asyncio
import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from api_clients import OpenAlexBudgetExceeded, OpenAlexClient, OpenCitationsClient
from scisieve.cli import build_parser
from scisieve.config import load_resolved_config
from scisieve.pipeline import (
    METADATA_COLUMNS,
    SCREENING_FT_COLUMNS,
    SCREENING_TA_COLUMNS,
    _assert_profile_guards,
    _build_scholarly_filter,
    _merge_metadata_rows,
    _record_id_for_work,
    _topic_classifier,
    create_context,
    run_pipeline,
    stage_anchor_check,
    stage_fulltext,
    stage_release,
    stage_screen_ta,
)


REPO_ROOT = Path(__file__).resolve().parent.parent


class VNextConfigTests(unittest.TestCase):
    def test_cli_accepts_profile_after_subcommand(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["run", "--profile", "debug", "--config", "scisieve.yaml"])
        self.assertEqual(args.command, "run")
        self.assertEqual(args.profile, "debug")

    def test_legacy_csur_profile_alias_maps_to_production(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="csur",
                run_root_override=tmp,
            )
            self.assertEqual(resolved.profile_name, "production")

    def test_load_resolved_config_uses_override_run_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            temp_root = Path(tmp)
            config_path = temp_root / "scisieve.yaml"
            config_text = (REPO_ROOT / "scisieve.yaml").read_text(encoding="utf-8")
            for name in (
                "query_packs.yaml",
                "gray_registry.yaml",
                "topic_profile.yaml",
                "anchor_benchmark.csv",
                "baseline_metadata.csv",
            ):
                config_text = config_text.replace(
                    f"examples/example_topic/{name}",
                    str((REPO_ROOT / "examples" / "example_topic" / name).resolve()).replace("\\", "/"),
                )
            config_path.write_text(config_text, encoding="utf-8")
            resolved = load_resolved_config(
                config_path=str(config_path),
                profile_name="debug",
                run_root_override=tmp,
            )
            self.assertEqual(resolved.paths.run_root, Path(tmp).resolve())
            self.assertTrue(resolved.resolved_end_date)
            self.assertTrue(resolved.paths.topic_profile_path.exists())
            self.assertEqual(resolved.openalex_api_key, "")

    def test_load_resolved_config_expands_env_var_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            topic_dir = Path(tmp) / "private_bundle"
            topic_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "query_packs.yaml",
                "gray_registry.yaml",
                "topic_profile.yaml",
                "anchor_benchmark.csv",
                "baseline_metadata.csv",
            ):
                (topic_dir / name).write_text(
                    (REPO_ROOT / "examples" / "example_topic" / name).read_text(encoding="utf-8"),
                    encoding="utf-8",
                )
            config_path = Path(tmp) / "scisieve.yaml"
            config_text = (REPO_ROOT / "scisieve.yaml").read_text(encoding="utf-8")
            config_text = config_text.replace(
                "examples/example_topic/query_packs.yaml",
                "${SCISIEVE_PRIVATE_ROOT}/query_packs.yaml",
            )
            config_text = config_text.replace(
                "examples/example_topic/gray_registry.yaml",
                "${SCISIEVE_PRIVATE_ROOT}/gray_registry.yaml",
            )
            config_text = config_text.replace(
                "examples/example_topic/topic_profile.yaml",
                "${SCISIEVE_PRIVATE_ROOT}/topic_profile.yaml",
            )
            config_text = config_text.replace(
                "examples/example_topic/anchor_benchmark.csv",
                "${SCISIEVE_PRIVATE_ROOT}/anchor_benchmark.csv",
            )
            config_text = config_text.replace(
                "examples/example_topic/baseline_metadata.csv",
                "${SCISIEVE_PRIVATE_ROOT}/baseline_metadata.csv",
            )
            config_path.write_text(config_text, encoding="utf-8")
            with patch.dict("os.environ", {"SCISIEVE_PRIVATE_ROOT": str(topic_dir.resolve())}):
                resolved = load_resolved_config(
                    config_path=str(config_path),
                    profile_name="debug",
                    run_root_override=tmp,
                )
            self.assertEqual(resolved.paths.query_packs_path, (topic_dir / "query_packs.yaml").resolve())
            self.assertEqual(resolved.paths.topic_profile_path, (topic_dir / "topic_profile.yaml").resolve())

    def test_build_scholarly_filter_respects_pack_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            filter_expression = _build_scholarly_filter(
                ctx,
                ["article", "proceedings-article", "preprint"],
                pack={"filters": {"publication_year_from": 2020, "types": ["article", "proceedings-article"]}},
            )
            self.assertIn("from_publication_date:2020-01-01", filter_expression)
            self.assertIn("type:article|proceedings-article", filter_expression)
            self.assertNotIn("preprint", filter_expression)

    def test_load_resolved_config_reads_default_openalex_api_key_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            temp_root = Path(tmp)
            config_path = temp_root / "scisieve.yaml"
            config_path.write_text((REPO_ROOT / "scisieve.yaml").read_text(encoding="utf-8"), encoding="utf-8")
            secret_dir = temp_root / ".scisieve_secrets"
            secret_dir.mkdir(parents=True, exist_ok=True)
            (secret_dir / "openalex_api_key.txt").write_text("secret-from-file\n", encoding="utf-8")
            resolved = load_resolved_config(
                config_path=str(config_path),
                profile_name="debug",
                run_root_override=tmp,
            )
            self.assertEqual(resolved.openalex_api_key, "secret-from-file")


class ApiClientTests(unittest.TestCase):
    def test_opencitations_pid_parsing(self) -> None:
        field = "omid:br/06203420991 doi:10.1109/hpcc.2011.111 openalex:W2172053398"
        self.assertEqual(OpenCitationsClient.extract_doi_from_pid_field(field), "10.1109/hpcc.2011.111")
        self.assertEqual(
            OpenCitationsClient.extract_openalex_id_from_pid_field(field),
            "https://openalex.org/W2172053398",
        )

    def test_openalex_get_work_normalizes_openalex_html_ids(self) -> None:
        client = OpenAlexClient(email="review@example.org")
        client._request_json = AsyncMock(return_value={"id": "https://openalex.org/W2172053398"})  # type: ignore[attr-defined]
        result = asyncio.run(client.get_work("https://openalex.org/W2172053398"))
        self.assertEqual(result["id"], "https://openalex.org/W2172053398")
        client._request_json.assert_awaited_once_with(  # type: ignore[attr-defined]
            "GET",
            "/works/W2172053398",
            params={"mailto": "review@example.org"},
        )
        asyncio.run(client.close())

    def test_openalex_budget_exhaustion_is_detected(self) -> None:
        client = OpenAlexClient(email="review@example.org")
        response = httpx.Response(
            429,
            json={
                "error": "Rate limit exceeded",
                "message": "Insufficient budget. This request costs $0.001 but you only have $0 remaining. Resets at midnight UTC.",
            },
        )
        with self.assertRaises(OpenAlexBudgetExceeded) as caught:
            client._raise_if_budget_exceeded(response)  # type: ignore[attr-defined]
        self.assertEqual(caught.exception.request_cost_usd, "0.001")
        self.assertEqual(caught.exception.remaining_budget_usd, "0")
        self.assertTrue(caught.exception.reset_at_utc)
        asyncio.run(client.close())

    def test_openalex_auth_params_include_api_key_when_present(self) -> None:
        client = OpenAlexClient(email="review@example.org", api_key="secret-key")
        params = client.auth_params({"per_page": 1})
        self.assertEqual(params["mailto"], "review@example.org")
        self.assertEqual(params["api_key"], "secret-key")
        self.assertEqual(params["per_page"], 1)
        self.assertEqual(OpenAlexClient.clamp_per_page(200), 100)
        asyncio.run(client.close())


class VNextClassifierTests(unittest.TestCase):
    def _context(self):
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            yield create_context(resolved)

    def test_topic_classifier_includes_archival_modeling_paper(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            row = {
                "title": "Formal analysis of orchestrated service reliability",
                "abstract_text_reconstructed": "We present formal analysis and model checking for reliability in cluster platform services.",
                "doi": "10.1000/test",
                "type": "article",
            }
            result = _topic_classifier(ctx, row)
            self.assertEqual(result["machine_decision"], "include")

    def test_topic_classifier_routes_survey_to_tertiary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            row = {
                "title": "A Survey of Reliability Assessment Methods",
                "abstract_text_reconstructed": "This survey reviews distributed service reliability evaluation methods.",
                "doi": "10.1000/survey",
                "type": "article",
            }
            result = _topic_classifier(ctx, row)
            self.assertEqual(result["machine_decision"], "tertiary_background")

    def test_topic_classifier_routes_discovery_only_anchor_to_tertiary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            row = {
                "title": "Reliability Assessment: A Literature Review",
                "abstract_text_reconstructed": "A literature review of reliability assessment methods in large-scale service systems.",
                "doi": "10.1000/review",
                "type": "article",
            }
            result = _topic_classifier(ctx, row)
            self.assertEqual(result["machine_decision"], "tertiary_background")

    def test_topic_classifier_includes_simulation_based_platform_analysis(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            row = {
                "title": "Simulation-based reliability analysis for orchestrated service platforms",
                "abstract_text_reconstructed": "We use a simulator for continuity assessment of orchestrated deployment failover behavior.",
                "doi": "10.1000/platform-sim",
                "type": "article",
            }
            result = _topic_classifier(ctx, row)
            self.assertEqual(result["machine_decision"], "include")

    def test_topic_classifier_applies_title_rescue_rule_for_title_only_archival_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            classifier_cfg = ctx.topic_profile_payload.setdefault("classifier", {})
            classifier_cfg["title_rescue_rules"] = [
                {
                    "id": "proactive_failure_risk_title",
                    "only_when_abstract_missing": True,
                    "match_query_pack_ids": ["pack_proactive_failure_risk"],
                    "require_title_groups": {
                        "context": ["cloud"],
                        "method": ["failure risk", "proactive actions", "risk based"],
                        "metric": ["reliability"],
                    },
                    "archival_decision": "include",
                    "preprint_decision": "preprint_watchlist",
                    "confidence": 0.82,
                }
            ]
            row = {
                "title": "Cloud reliability and efficiency improvement via failure risk based proactive actions",
                "abstract_text_reconstructed": "",
                "query_pack_ids": "pack_proactive_failure_risk",
                "doi": "10.1000/proactive-risk",
                "type": "article",
            }
            result = _topic_classifier(ctx, row)
            self.assertEqual(result["machine_decision"], "include")
            self.assertIn("title_rescue_rule=proactive_failure_risk_title", result["machine_reason"])
            self.assertIn("context=cloud", result["machine_reason"])


class VNextStageTests(unittest.TestCase):
    def test_merge_metadata_rows_collapses_same_title_with_different_doi(self) -> None:
        existing = [
            {
                "record_id": "rec1",
                "track": "scholarly_core",
                "archival_status": "archival",
                "source": "Snowballing",
                "discovery_mode": "snowball",
                "query_pack_ids": "pack_alpha",
                "query_terms_triggered": "reliability",
                "anchor_match_ids": "",
                "negative_sentinel_match": "",
                "openalex_id": "https://openalex.org/W1",
                "doi": "10.1000/a",
                "title": "An energy-aware fault tolerant scheduling framework",
                "publication_year": "2024",
                "publication_date": "2024-01-01",
                "type": "article",
                "primary_source_name": "Venue A",
                "cited_by_count": "7",
                "best_pdf_url": "",
                "label_primary_dimension": "approach_family",
                "label_primary_value": "Simulation",
                "label_secondary_dimension": "deployment_scope",
                "label_secondary_value": "OrchestratedPlatform",
                "topic_labels_json": "{}",
                "machine_decision": "include",
                "machine_confidence": "0.70",
                "machine_reason": "first",
                "matched_archival_id": "",
                "matched_archival_doi": "",
                "retrieval_score_norm": "0.70",
            }
        ]
        additional = [
            {
                "record_id": "rec2",
                "track": "scholarly_core",
                "archival_status": "archival",
                "source": "Snowballing",
                "discovery_mode": "snowball",
                "query_pack_ids": "pack_beta",
                "query_terms_triggered": "fault injection",
                "anchor_match_ids": "",
                "negative_sentinel_match": "",
                "openalex_id": "https://openalex.org/W2",
                "doi": "10.1000/b",
                "title": "An energy-aware fault tolerant scheduling framework",
                "publication_year": "2025",
                "publication_date": "2025-01-01",
                "type": "article",
                "primary_source_name": "Venue B",
                "cited_by_count": "15",
                "best_pdf_url": "",
                "label_primary_dimension": "approach_family",
                "label_primary_value": "Simulation",
                "label_secondary_dimension": "deployment_scope",
                "label_secondary_value": "OrchestratedPlatform",
                "topic_labels_json": "{}",
                "machine_decision": "include",
                "machine_confidence": "0.85",
                "machine_reason": "second",
                "matched_archival_id": "",
                "matched_archival_doi": "",
                "retrieval_score_norm": "0.80",
            }
        ]

        merged = _merge_metadata_rows(existing, additional)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["openalex_id"], "https://openalex.org/W2")
        self.assertIn("pack_alpha", merged[0]["query_pack_ids"])
        self.assertIn("pack_beta", merged[0]["query_pack_ids"])

    def test_stage_fulltext_includes_needs_fulltext_queue_rows_when_profile_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            (ctx.config.paths.run_root / "metadata.csv").write_text(
                ",".join(METADATA_COLUMNS) + "\n",
                encoding="utf-8",
            )
            queue_row = {
                "record_id": "rec_queue_a10",
                "openalex_id": "https://openalex.org/W3001891115",
                "doi": "10.1016/j.jss.2020.110524",
                "title": "Cloud reliability and efficiency improvement via failure risk based proactive actions",
                "year": "2020",
                "query_pack_ids": "pack_proactive_failure_risk",
                "anchor_match_ids": "A10",
                "negative_sentinel_match": "",
                "machine_decision": "needs_fulltext",
                "machine_confidence": "0.75",
                "machine_reason": "anchor_match=A10",
                "reviewer1_decision": "",
                "reviewer1_reason": "",
                "reviewer2_decision": "",
                "reviewer2_reason": "",
                "resolved_decision": "needs_fulltext",
                "resolved_reason": "",
                "decision_stage_timestamp": "2026-04-13T00:00:00Z",
                "notes": "",
                "pdf_local_path": "",
                "pdf_parse_status": "not_attempted",
                "fulltext_sections_seen": "",
                "equations_detected": "",
                "figures_detected": "",
            }
            with (ctx.config.paths.run_root / "screening_fulltext.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=SCREENING_FT_COLUMNS)
                writer.writeheader()
                writer.writerow(queue_row)

            asyncio.run(stage_fulltext(ctx))

            inventory = (ctx.config.paths.run_root / "fulltext_inventory.csv").read_text(encoding="utf-8")
            self.assertIn("rec_queue_a10", inventory)
            self.assertIn("needs_fulltext_queue", inventory)
            self.assertIn("disabled_by_profile", inventory)

    def test_stage_screen_ta_writes_expected_tracks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            ctx.config.paths.normalized_dir.mkdir(parents=True, exist_ok=True)

            normalized_rows = [
                {
                    "openalex_id": "https://openalex.org/W1",
                    "doi": "10.1000/include",
                    "title": "Formal analysis of orchestrated service reliability",
                    "abstract_text_reconstructed": "We present formal analysis and model checking for reliability in cluster platform services.",
                    "publication_year": 2024,
                    "publication_date": "2024-05-01",
                    "type": "article",
                    "query_pack_ids": "pack_foundation_formal",
                    "query_terms_triggered": "formal analysis;model checking;cluster platform",
                    "primary_source_name": "Test Venue",
                    "retrieval_score_norm": "0.9",
                    "raw_work": {},
                },
                {
                    "openalex_id": "https://openalex.org/W2",
                    "doi": "10.1000/tertiary",
                    "title": "A Survey of Reliability Assessment Methods",
                    "abstract_text_reconstructed": "This survey reviews distributed service reliability evaluation methods.",
                    "publication_year": 2021,
                    "publication_date": "2021-01-01",
                    "type": "article",
                    "query_pack_ids": "pack_review_discovery",
                    "query_terms_triggered": "survey;distributed service;reliability",
                    "primary_source_name": "Review Venue",
                    "retrieval_score_norm": "0.8",
                    "raw_work": {},
                },
                {
                    "openalex_id": "https://openalex.org/W3",
                    "doi": "10.1000/preprint",
                    "title": "Fault injection for orchestrated deployment continuity",
                    "abstract_text_reconstructed": "We use fault injection to estimate recovery behavior in an orchestrated deployment platform.",
                    "publication_year": 2025,
                    "publication_date": "2025-02-01",
                    "type": "preprint",
                    "query_pack_ids": "pack_resilience_experiments",
                    "query_terms_triggered": "fault injection;orchestrated deployment;recovery",
                    "primary_source_name": "",
                    "retrieval_score_norm": "0.7",
                    "raw_work": {},
                },
            ]
            (ctx.config.paths.normalized_dir / "normalized_candidates.jsonl").write_text(
                "\n".join(json.dumps(row) for row in normalized_rows) + "\n",
                encoding="utf-8",
            )
            dedup_payload = {
                "core": [
                    {
                        "source": "OpenAlex Query Packs",
                        "openalex_id": "https://openalex.org/W1",
                        "title": normalized_rows[0]["title"],
                        "doi": normalized_rows[0]["doi"],
                        "type": "article",
                        "publication_date": normalized_rows[0]["publication_date"],
                        "publication_year": normalized_rows[0]["publication_year"],
                        "track": "core",
                        "archival_status": "archival",
                        "is_in_core_corpus": True,
                        "is_watchlist_candidate": False,
                        "watchlist_tag": "",
                        "replaced_by_doi": "",
                        "matched_archival_id": "",
                        "matched_archival_doi": "",
                        "resolved_from_preprint": False,
                        "label_primary_dimension": "approach_family",
                        "label_primary_value": "Formal",
                        "label_secondary_dimension": "deployment_scope",
                        "label_secondary_value": "OrchestratedPlatform",
                        "topic_labels_json": "{\"approach_family\":\"Formal\",\"deployment_scope\":\"OrchestratedPlatform\"}",
                        "cited_by_count": 10,
                        "best_pdf_url": "",
                        "abstract": normalized_rows[0]["abstract_text_reconstructed"],
                    },
                    {
                        "source": "OpenAlex Query Packs",
                        "openalex_id": "https://openalex.org/W2",
                        "title": normalized_rows[1]["title"],
                        "doi": normalized_rows[1]["doi"],
                        "type": "article",
                        "publication_date": normalized_rows[1]["publication_date"],
                        "publication_year": normalized_rows[1]["publication_year"],
                        "track": "core",
                        "archival_status": "archival",
                        "is_in_core_corpus": True,
                        "is_watchlist_candidate": False,
                        "watchlist_tag": "",
                        "replaced_by_doi": "",
                        "matched_archival_id": "",
                        "matched_archival_doi": "",
                        "resolved_from_preprint": False,
                        "label_primary_dimension": "approach_family",
                        "label_primary_value": "Learning",
                        "label_secondary_dimension": "deployment_scope",
                        "label_secondary_value": "Unknown",
                        "topic_labels_json": "{\"approach_family\":\"Learning\",\"deployment_scope\":\"Unknown\"}",
                        "cited_by_count": 15,
                        "best_pdf_url": "",
                        "abstract": normalized_rows[1]["abstract_text_reconstructed"],
                    },
                ],
                "watchlist": [
                    {
                        "source": "OpenAlex Query Packs",
                        "openalex_id": "https://openalex.org/W3",
                        "title": normalized_rows[2]["title"],
                        "doi": normalized_rows[2]["doi"],
                        "type": "preprint",
                        "publication_date": normalized_rows[2]["publication_date"],
                        "publication_year": normalized_rows[2]["publication_year"],
                        "track": "preprint_watchlist",
                        "archival_status": "preprint_watchlist",
                        "is_in_core_corpus": False,
                        "is_watchlist_candidate": True,
                        "watchlist_tag": "",
                        "replaced_by_doi": "",
                        "matched_archival_id": "",
                        "matched_archival_doi": "",
                        "resolved_from_preprint": False,
                        "label_primary_dimension": "approach_family",
                        "label_primary_value": "Experimental",
                        "label_secondary_dimension": "deployment_scope",
                        "label_secondary_value": "OrchestratedPlatform",
                        "topic_labels_json": "{\"approach_family\":\"Experimental\",\"deployment_scope\":\"OrchestratedPlatform\"}",
                        "cited_by_count": 5,
                        "best_pdf_url": "",
                        "abstract": normalized_rows[2]["abstract_text_reconstructed"],
                    }
                ],
            }
            (ctx.config.paths.normalized_dir / "deduped_candidates.json").write_text(
                json.dumps(dedup_payload),
                encoding="utf-8",
            )

            stage_screen_ta(ctx)
            metadata = (ctx.config.paths.run_root / "metadata.csv").read_text(encoding="utf-8")
            self.assertIn("scholarly_core", metadata)
            self.assertIn("scholarly_tertiary", metadata)
            self.assertIn("preprint_watchlist", metadata)
            self.assertIn("label_primary_dimension", metadata.splitlines()[0])

    def test_release_blocks_on_coverage_failures_for_production(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="production",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            (ctx.config.paths.run_root / "coverage_failures.csv").write_text(
                "anchor_id,tier,diagnosis,action_required\nA01,A,not_retrieved,True\n",
                encoding="utf-8",
            )
            with self.assertRaises(RuntimeError):
                stage_release(ctx)

    def test_profile_guard_rejects_capped_production_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            topic_dir = Path(tmp) / "examples" / "example_topic"
            topic_dir.mkdir(parents=True, exist_ok=True)
            (topic_dir / "query_packs.yaml").write_text(
                (REPO_ROOT / "examples" / "example_topic" / "query_packs.yaml").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            (topic_dir / "gray_registry.yaml").write_text(
                (REPO_ROOT / "examples" / "example_topic" / "gray_registry.yaml").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            (topic_dir / "topic_profile.yaml").write_text(
                (REPO_ROOT / "examples" / "example_topic" / "topic_profile.yaml").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            (topic_dir / "anchor_benchmark.csv").write_text(
                (REPO_ROOT / "examples" / "example_topic" / "anchor_benchmark.csv").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            (topic_dir / "baseline_metadata.csv").write_text(
                (REPO_ROOT / "examples" / "example_topic" / "baseline_metadata.csv").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            config_path = Path(tmp) / "scisieve.yaml"
            config_text = (REPO_ROOT / "scisieve.yaml").read_text(encoding="utf-8")
            config_text = config_text.replace("max_records_per_pack:\n", "max_records_per_pack: 10\n", 1)
            config_text = config_text.replace(
                "examples/example_topic/query_packs.yaml",
                str((topic_dir / "query_packs.yaml").resolve()).replace("\\", "/"),
            )
            config_text = config_text.replace(
                "examples/example_topic/gray_registry.yaml",
                str((topic_dir / "gray_registry.yaml").resolve()).replace("\\", "/"),
            )
            config_text = config_text.replace(
                "examples/example_topic/anchor_benchmark.csv",
                str((topic_dir / "anchor_benchmark.csv").resolve()).replace("\\", "/"),
            )
            config_text = config_text.replace(
                "examples/example_topic/baseline_metadata.csv",
                str((topic_dir / "baseline_metadata.csv").resolve()).replace("\\", "/"),
            )
            config_text = config_text.replace(
                "examples/example_topic/topic_profile.yaml",
                str((topic_dir / "topic_profile.yaml").resolve()).replace("\\", "/"),
            )
            config_path.write_text(config_text, encoding="utf-8")
            resolved = load_resolved_config(
                config_path=str(config_path),
                profile_name="production",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            with self.assertRaises(RuntimeError):
                _assert_profile_guards(ctx)

    def test_anchor_check_counts_snowball_retrieval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            ctx.anchor_rows = [
                {
                    "anchor_id": "A_TEST",
                    "polarity": "positive",
                    "tier": "A",
                    "expected_stage": "include",
                    "anchor_title": "Failure benchmarks for orchestrated service control layers",
                    "doi": "10.1000/failure-benchmark",
                }
            ]
            (ctx.config.paths.normalized_dir / "normalized_candidates.jsonl").write_text("", encoding="utf-8")
            (ctx.config.paths.run_root / "metadata.csv").write_text(
                ",".join(METADATA_COLUMNS) + "\n",
                encoding="utf-8",
            )
            record_id = _record_id_for_work(
                "https://openalex.org/W999",
                "10.1000/failure-benchmark",
                "Failure benchmarks for orchestrated service control layers",
            )
            screening_row = {
                "record_id": record_id,
                "openalex_id": "https://openalex.org/W999",
                "doi": "10.1000/failure-benchmark",
                "title": "Failure benchmarks for orchestrated service control layers",
                "year": "2024",
                "query_pack_ids": "pack_resilience_experiments",
                "anchor_match_ids": "A_TEST",
                "negative_sentinel_match": "",
                "machine_decision": "needs_fulltext",
                "machine_confidence": "0.84",
                "machine_reason": "anchor_match=A_TEST",
                "reviewer1_decision": "",
                "reviewer1_reason": "",
                "reviewer2_decision": "",
                "reviewer2_reason": "",
                "resolved_decision": "needs_fulltext",
                "resolved_reason": "",
                "decision_stage_timestamp": "2026-04-12T00:00:00Z",
                "notes": "snowball_round=1",
            }
            with (ctx.config.paths.run_root / "screening_title_abstract.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=SCREENING_TA_COLUMNS)
                writer.writeheader()
                writer.writerow(screening_row)
            (ctx.config.paths.run_root / "snowball_included.csv").write_text(
                ",".join(METADATA_COLUMNS) + "\n",
                encoding="utf-8",
            )
            with (ctx.config.paths.run_root / "snowball_excluded.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=SCREENING_TA_COLUMNS)
                writer.writeheader()
                writer.writerow(screening_row)
            asyncio.run(stage_anchor_check(ctx))
            coverage = (ctx.config.paths.run_root / "coverage_anchor_results.csv").read_text(encoding="utf-8")
            self.assertIn("A_TEST,positive,A,include,True,True,pack_resilience_experiments,screen_ft_excluded", coverage)
            self.assertIn("needs_fulltext_queue", coverage)

    def test_anchor_check_treats_snowball_included_as_discovered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            ctx.anchor_rows = [
                {
                    "anchor_id": "A_SNOW",
                    "polarity": "positive",
                    "tier": "A",
                    "expected_stage": "include",
                    "anchor_title": "Snowball discovered service resilience paper",
                    "doi": "10.1000/snowball-hit",
                }
            ]
            (ctx.config.paths.normalized_dir / "normalized_candidates.jsonl").write_text("", encoding="utf-8")
            (ctx.config.paths.run_root / "metadata.csv").write_text(",".join(METADATA_COLUMNS) + "\n", encoding="utf-8")
            (ctx.config.paths.run_root / "screening_title_abstract.csv").write_text(
                ",".join(SCREENING_TA_COLUMNS) + "\n",
                encoding="utf-8",
            )
            snowball_row = {
                "record_id": "rec_snow",
                "track": "scholarly_core",
                "archival_status": "archival",
                "source": "Snowballing",
                "discovery_mode": "snowball",
                "query_pack_ids": "pack_review_discovery",
                "query_terms_triggered": "resilien*;cloud computing",
                "anchor_match_ids": "A_SNOW",
                "negative_sentinel_match": "",
                "openalex_id": "https://openalex.org/W777",
                "doi": "10.1000/snowball-hit",
                "title": "Snowball discovered service resilience paper",
                "publication_year": "2024",
                "publication_date": "2024-01-01",
                "type": "article",
                "primary_source_name": "Venue",
                "cited_by_count": "3",
                "best_pdf_url": "",
                "label_primary_dimension": "approach_family",
                "label_primary_value": "Analytics",
                "label_secondary_dimension": "deployment_scope",
                "label_secondary_value": "Cloud",
                "topic_labels_json": "{}",
                "machine_decision": "include",
                "machine_confidence": "0.90",
                "machine_reason": "snowball include",
                "matched_archival_id": "",
                "matched_archival_doi": "",
                "retrieval_score_norm": "1.0",
            }
            with (ctx.config.paths.run_root / "snowball_included.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=METADATA_COLUMNS)
                writer.writeheader()
                writer.writerow(snowball_row)
            (ctx.config.paths.run_root / "snowball_excluded.csv").write_text(
                ",".join(SCREENING_TA_COLUMNS) + "\n",
                encoding="utf-8",
            )
            asyncio.run(stage_anchor_check(ctx))
            coverage = (ctx.config.paths.run_root / "coverage_anchor_results.csv").read_text(encoding="utf-8")
            self.assertIn("A_SNOW,positive,A,include,True,True,pack_review_discovery,,scholarly_core,rec_snow,discovered,False", coverage)

    def test_anchor_check_uses_snowball_excluded_screening_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            ctx.anchor_rows = [
                {
                    "anchor_id": "A_TERT",
                    "polarity": "positive",
                    "tier": "A",
                    "expected_stage": "discovery_only_or_tertiary",
                    "anchor_title": "Snowball tertiary review paper",
                    "doi": "10.1000/snowball-tertiary",
                }
            ]
            (ctx.config.paths.normalized_dir / "normalized_candidates.jsonl").write_text("", encoding="utf-8")
            (ctx.config.paths.run_root / "metadata.csv").write_text(",".join(METADATA_COLUMNS) + "\n", encoding="utf-8")
            (ctx.config.paths.run_root / "screening_title_abstract.csv").write_text(
                ",".join(SCREENING_TA_COLUMNS) + "\n",
                encoding="utf-8",
            )
            (ctx.config.paths.run_root / "snowball_included.csv").write_text(
                ",".join(METADATA_COLUMNS) + "\n",
                encoding="utf-8",
            )
            screening_row = {
                "record_id": "rec_tert",
                "openalex_id": "https://openalex.org/W778",
                "doi": "10.1000/snowball-tertiary",
                "title": "Snowball tertiary review paper",
                "year": "2024",
                "query_pack_ids": "pack_review_discovery",
                "anchor_match_ids": "A_TERT",
                "negative_sentinel_match": "",
                "machine_decision": "tertiary_background",
                "machine_confidence": "0.86",
                "machine_reason": "review",
                "reviewer1_decision": "",
                "reviewer1_reason": "",
                "reviewer2_decision": "",
                "reviewer2_reason": "",
                "resolved_decision": "tertiary_background",
                "resolved_reason": "",
                "decision_stage_timestamp": "2026-04-13T00:00:00Z",
                "notes": "snowball_round=1",
            }
            with (ctx.config.paths.run_root / "snowball_excluded.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=SCREENING_TA_COLUMNS)
                writer.writeheader()
                writer.writerow(screening_row)
            asyncio.run(stage_anchor_check(ctx))
            coverage = (ctx.config.paths.run_root / "coverage_anchor_results.csv").read_text(encoding="utf-8")
            self.assertIn("A_TERT,positive,A,discovery_only_or_tertiary,True,True,pack_review_discovery,tertiary_routed,,", coverage)
            self.assertIn("tertiary_background,False", coverage)

    def test_run_pipeline_resumes_from_paused_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            (ctx.config.paths.run_root / "resume_state.json").write_text(
                json.dumps(
                    {
                        "status": "paused_budget",
                        "stage": "snowball",
                        "detail": "budget exhausted",
                        "resume_after_utc": "2026-04-13T00:00:00+00:00",
                    }
                ),
                encoding="utf-8",
            )
            recorded: list[str] = []

            async def _fake_run_stage(_ctx, stage_name: str) -> None:
                recorded.append(stage_name)

            with patch("scisieve.pipeline._run_named_stage", side_effect=_fake_run_stage):
                asyncio.run(run_pipeline(ctx))
            self.assertEqual(
                recorded,
                ["snowball", "fulltext", "extract", "quality", "gray", "anchor_check", "audit", "release"],
            )


if __name__ == "__main__":
    unittest.main()

