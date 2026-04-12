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
from scicrawl.cli import build_parser
from scicrawl.config import load_resolved_config
from scicrawl.pipeline import (
    METADATA_COLUMNS,
    SCREENING_TA_COLUMNS,
    _assert_profile_guards,
    _build_scholarly_filter,
    _record_id_for_work,
    _topic_classifier,
    create_context,
    run_pipeline,
    stage_anchor_check,
    stage_release,
    stage_screen_ta,
)


REPO_ROOT = Path(__file__).resolve().parent.parent


class VNextConfigTests(unittest.TestCase):
    def test_cli_accepts_profile_after_subcommand(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["run", "--profile", "debug", "--config", "scicrawl.yaml"])
        self.assertEqual(args.command, "run")
        self.assertEqual(args.profile, "debug")

    def test_load_resolved_config_uses_override_run_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scicrawl.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            self.assertEqual(resolved.paths.run_root, Path(tmp).resolve())
            self.assertTrue(resolved.resolved_end_date)
            self.assertTrue(resolved.paths.topic_profile_path.exists())

    def test_build_scholarly_filter_respects_pack_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scicrawl.yaml"),
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


class VNextClassifierTests(unittest.TestCase):
    def _context(self):
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scicrawl.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            yield create_context(resolved)

    def test_topic_classifier_includes_archival_modeling_paper(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scicrawl.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            row = {
                "title": "Availability modeling for cloud-native microservices",
                "abstract_text_reconstructed": "We present probabilistic model checking for resilience and availability in kubernetes services.",
                "doi": "10.1000/test",
                "type": "article",
            }
            result = _topic_classifier(ctx, row)
            self.assertEqual(result["machine_decision"], "include")

    def test_topic_classifier_routes_survey_to_tertiary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scicrawl.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            row = {
                "title": "A Survey of AIOps Methods for Failure Management",
                "abstract_text_reconstructed": "This survey reviews cloud failure management and reliability methods.",
                "doi": "10.1000/survey",
                "type": "article",
            }
            result = _topic_classifier(ctx, row)
            self.assertEqual(result["machine_decision"], "tertiary_background")

    def test_topic_classifier_routes_discovery_only_anchor_to_tertiary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scicrawl.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            row = {
                "title": "Chaos Engineering",
                "abstract_text_reconstructed": "An engineering perspective on controlled experiments in large-scale systems.",
                "doi": "10.1109/MS.2016.60",
                "type": "article",
            }
            result = _topic_classifier(ctx, row)
            self.assertEqual(result["machine_decision"], "tertiary_background")

    def test_topic_classifier_includes_openstack_availability_modeling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scicrawl.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            row = {
                "title": "Availability modeling in OpenStack private clouds",
                "abstract_text_reconstructed": "We use a simulator for resilience assessment of IaaS failover behavior.",
                "doi": "10.1000/openstack",
                "type": "article",
            }
            result = _topic_classifier(ctx, row)
            self.assertEqual(result["machine_decision"], "include")


class VNextStageTests(unittest.TestCase):
    def test_stage_screen_ta_writes_expected_tracks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scicrawl.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            ctx.config.paths.normalized_dir.mkdir(parents=True, exist_ok=True)

            normalized_rows = [
                {
                    "openalex_id": "https://openalex.org/W1",
                    "doi": "10.1000/include",
                    "title": "Availability modeling for cloud-native microservices",
                    "abstract_text_reconstructed": "We present probabilistic model checking for resilience and availability in kubernetes services.",
                    "publication_year": 2024,
                    "publication_date": "2024-05-01",
                    "type": "article",
                    "query_pack_ids": "pack_formal_methods",
                    "query_terms_triggered": "availability;model checking;kubernetes",
                    "primary_source_name": "Test Venue",
                    "retrieval_score_norm": "0.9",
                    "raw_work": {},
                },
                {
                    "openalex_id": "https://openalex.org/W2",
                    "doi": "10.1000/tertiary",
                    "title": "A Survey of AIOps Methods for Failure Management",
                    "abstract_text_reconstructed": "This survey reviews cloud failure management and reliability methods.",
                    "publication_year": 2021,
                    "publication_date": "2021-01-01",
                    "type": "article",
                    "query_pack_ids": "pack_review_discovery",
                    "query_terms_triggered": "survey;cloud;reliability",
                    "primary_source_name": "Review Venue",
                    "retrieval_score_norm": "0.8",
                    "raw_work": {},
                },
                {
                    "openalex_id": "https://openalex.org/W3",
                    "doi": "10.1000/preprint",
                    "title": "Fault injection for serverless resilience",
                    "abstract_text_reconstructed": "We use fault injection to estimate recovery behavior in serverless cloud services.",
                    "publication_year": 2025,
                    "publication_date": "2025-02-01",
                    "type": "preprint",
                    "query_pack_ids": "pack_chaos_fault_injection",
                    "query_terms_triggered": "fault injection;serverless;recovery",
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
                        "resilience_paradigm": "Formal",
                        "cloud_context": "K8s",
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
                        "resilience_paradigm": "ML",
                        "cloud_context": "GeneralCloud",
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
                        "resilience_paradigm": "Chaos",
                        "cloud_context": "Serverless",
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

    def test_release_blocks_on_coverage_failures_for_csur(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scicrawl.yaml"),
                profile_name="csur",
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
            topic_dir = Path(tmp) / "topics" / "cloud_resilience_dependability"
            topic_dir.mkdir(parents=True, exist_ok=True)
            (topic_dir / "query_packs.yaml").write_text(
                (REPO_ROOT / "topics" / "cloud_resilience_dependability" / "query_packs.yaml").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            (topic_dir / "gray_registry.yaml").write_text(
                (REPO_ROOT / "topics" / "cloud_resilience_dependability" / "gray_registry.yaml").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            config_path = Path(tmp) / "scicrawl.yaml"
            config_text = (REPO_ROOT / "scicrawl.yaml").read_text(encoding="utf-8")
            config_text = config_text.replace("max_records_per_pack:\n", "max_records_per_pack: 10\n", 1)
            config_text = config_text.replace(
                "topics/cloud_resilience_dependability/query_packs.yaml",
                str((topic_dir / "query_packs.yaml").resolve()).replace("\\", "/"),
            )
            config_text = config_text.replace(
                "topics/cloud_resilience_dependability/gray_registry.yaml",
                str((topic_dir / "gray_registry.yaml").resolve()).replace("\\", "/"),
            )
            config_text = config_text.replace(
                "reference/scicrawl_anchor_benchmark_v1.csv",
                str((REPO_ROOT / "reference" / "scicrawl_anchor_benchmark_v1.csv").resolve()).replace("\\", "/"),
            )
            config_text = config_text.replace(
                "reference/baseline_metadata.csv",
                str((REPO_ROOT / "reference" / "baseline_metadata.csv").resolve()).replace("\\", "/"),
            )
            config_text = config_text.replace(
                "topics/cloud_resilience_dependability/topic_profile.yaml",
                str((REPO_ROOT / "topics" / "cloud_resilience_dependability" / "topic_profile.yaml").resolve()).replace("\\", "/"),
            )
            config_path.write_text(config_text, encoding="utf-8")
            resolved = load_resolved_config(
                config_path=str(config_path),
                profile_name="csur",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            with self.assertRaises(RuntimeError):
                _assert_profile_guards(ctx)

    def test_anchor_check_counts_snowball_retrieval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scicrawl.yaml"),
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
                    "anchor_title": "Mutiny! How Does Kubernetes Fail, and What Can We Do About It?",
                    "doi": "10.1109/DSN58291.2024.00016",
                }
            ]
            (ctx.config.paths.normalized_dir / "normalized_candidates.jsonl").write_text("", encoding="utf-8")
            (ctx.config.paths.run_root / "metadata.csv").write_text(
                ",".join(METADATA_COLUMNS) + "\n",
                encoding="utf-8",
            )
            record_id = _record_id_for_work(
                "https://openalex.org/W999",
                "10.1109/DSN58291.2024.00016",
                "Mutiny! How Does Kubernetes Fail, and What Can We Do About It?",
            )
            screening_row = {
                "record_id": record_id,
                "openalex_id": "https://openalex.org/W999",
                "doi": "10.1109/DSN58291.2024.00016",
                "title": "Mutiny! How Does Kubernetes Fail, and What Can We Do About It?",
                "year": "2024",
                "query_pack_ids": "pack_kubernetes_failure_modes",
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
            self.assertIn("A_TEST,positive,A,include,True,True,pack_kubernetes_failure_modes,screen_ft_excluded", coverage)
            self.assertIn("needs_fulltext_queue", coverage)

    def test_run_pipeline_resumes_from_paused_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scicrawl.yaml"),
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

            with patch("scicrawl.pipeline._run_named_stage", side_effect=_fake_run_stage):
                asyncio.run(run_pipeline(ctx))
            self.assertEqual(
                recorded,
                ["snowball", "fulltext", "extract", "quality", "gray", "anchor_check", "audit", "release"],
            )


if __name__ == "__main__":
    unittest.main()
