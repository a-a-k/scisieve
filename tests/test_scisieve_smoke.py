from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scisieve.common import read_csv
from scisieve.config import load_resolved_config
from scisieve.pipeline import create_context, execute


REPO_ROOT = Path(__file__).resolve().parent.parent


class OfflineSmokeTests(unittest.TestCase):
    def test_offline_smoke_pipeline_from_frozen_raw_to_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            resolved = load_resolved_config(
                config_path=str(REPO_ROOT / "scisieve.yaml"),
                profile_name="debug",
                run_root_override=tmp,
            )
            ctx = create_context(resolved)
            ctx.config.profile.snowball.enabled = False
            ctx.config.profile.fulltext.enabled = False
            ctx.config.profile.gray.enabled = False
            ctx.anchor_rows = [
                {
                    "anchor_id": "A_SMOKE",
                    "polarity": "positive",
                    "tier": "A",
                    "expected_stage": "include",
                    "anchor_title": "Availability modeling for cloud-native microservices",
                    "doi": "10.1000/smoke",
                    "query_pack_hint": "pack_formal_methods",
                }
            ]

            raw_entry = {
                "query_pack_id": "pack_formal_methods",
                "stream": "scholarly",
                "source_name": "OpenAlex",
                "retrieved_at_utc": ctx.config.run_timestamp_utc,
                "snapshot_id": ctx.config.profile.snapshot_id,
                "filter_expression": "type:article",
                "search_query": "availability modeling",
                "page_cursor": "*",
                "page_index": 1,
                "retrieval_order": 1,
                "work": {
                    "id": "https://openalex.org/WSMOKE1",
                    "doi": "10.1000/smoke",
                    "title": "Availability modeling for cloud-native microservices",
                    "abstract_inverted_index": {
                        "We": [0],
                        "present": [1],
                        "probabilistic": [2],
                        "model": [3],
                        "checking": [4],
                        "for": [5],
                        "resilience": [6],
                        "and": [7],
                        "availability": [8],
                        "in": [9],
                        "kubernetes": [10],
                        "services": [11],
                    },
                    "publication_year": 2024,
                    "publication_date": "2024-05-01",
                    "type": "article",
                    "language": "en",
                    "cited_by_count": 12,
                    "best_oa_location": {"pdf_url": ""},
                    "primary_location": {
                        "pdf_url": "",
                        "source": {
                            "display_name": "Smoke Test Venue",
                            "type": "journal",
                            "is_core": True,
                            "is_in_doaj": False,
                            "host_organization_name": "Smoke Publisher",
                        },
                    },
                },
            }
            raw_path = ctx.config.paths.raw_scholarly_dir / "pack-formal-methods__scholarly.jsonl"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(json.dumps(raw_entry, ensure_ascii=False) + "\n", encoding="utf-8")
            (ctx.config.paths.run_root / "search_strings.txt").write_text(
                "QUERY_PACKS\n[pack_formal_methods]\navailability modeling\n",
                encoding="utf-8",
            )

            execute("retrieve", ctx, target="scholarly")
            execute("normalize", ctx)
            execute("dedup", ctx)
            execute("screen-ta", ctx)
            execute("snowball", ctx)
            execute("fulltext", ctx)
            execute("extract", ctx)
            execute("quality", ctx)
            execute("gray", ctx)
            execute("anchor-check", ctx)
            execute("audit", ctx)
            execute("release", ctx)

            metadata_rows = read_csv(ctx.config.paths.run_root / "metadata.csv")
            self.assertEqual(len(metadata_rows), 1)
            self.assertEqual(metadata_rows[0]["track"], "scholarly_core")
            self.assertEqual(metadata_rows[0]["label_primary_dimension"], "resilience_paradigm")
            self.assertEqual(metadata_rows[0]["label_primary_value"], "Formal")

            coverage_rows = read_csv(ctx.config.paths.run_root / "coverage_anchor_results.csv")
            self.assertEqual(len(coverage_rows), 1)
            self.assertEqual(coverage_rows[0]["retrieved"], "True")

            release_manifest = ctx.config.paths.release_dir / "release_manifest.json"
            self.assertTrue(release_manifest.exists())
            self.assertTrue((ctx.config.paths.release_dir / "metadata.csv").exists())
            self.assertTrue((ctx.config.paths.release_dir / "scisieve.yaml").exists())


if __name__ == "__main__":
    unittest.main()
