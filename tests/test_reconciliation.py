import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from models import ResearchWork
from scisieve.config import load_resolved_config
from scisieve.pipeline import _deduplicate_tracks, _resolve_preprints_against_core, create_context


def make_work(
    *,
    work_id: str,
    title: str,
    work_type: str,
    doi: str | None,
    track: str,
    publication_date: str = "2024-01-01",
    cited_by_count: int = 0,
) -> ResearchWork:
    return ResearchWork(
        source="test",
        openalex_id=work_id,
        title=title,
        doi=doi,
        type=work_type,
        publication_date=publication_date,
        publication_year=int(publication_date[:4]),
        track=track,
        archival_status="archival" if work_type != "preprint" else "preprint_watchlist",
        is_in_core_corpus=track == "core",
        is_watchlist_candidate=track == "preprint_watchlist",
        watchlist_tag="watchlist" if track == "preprint_watchlist" else None,
        label_primary_dimension="approach_family",
        label_primary_value="Formal",
        label_secondary_dimension="deployment_scope",
        label_secondary_value="OrchestratedPlatform",
        topic_labels_json='{"approach_family":"Formal","deployment_scope":"OrchestratedPlatform"}',
        cited_by_count=cited_by_count,
        abstract="formal analysis for distributed service reliability",
    )


class ReconciliationTests(unittest.IsolatedAsyncioTestCase):
    def _context(self):
        tmp = tempfile.TemporaryDirectory()
        resolved = load_resolved_config(
            config_path="scisieve.yaml",
            profile_name="debug",
            run_root_override=tmp.name,
        )
        ctx = create_context(resolved)
        self.addAsyncCleanup(tmp.cleanup)
        return ctx

    async def test_same_doi_collapses_to_archival_core(self) -> None:
        article = make_work(
            work_id="https://openalex.org/W1",
            title="Formal analysis of orchestrated service reliability",
            work_type="article",
            doi="10.1000/example",
            track="core",
        )
        preprint = make_work(
            work_id="https://openalex.org/W2",
            title="Formal analysis of orchestrated service reliability",
            work_type="preprint",
            doi="10.1000/example",
            track="preprint_watchlist",
        )

        stats = _deduplicate_tracks([article], [preprint])
        self.assertEqual(len(stats.core_works), 1)
        self.assertEqual(len(stats.watchlist_works), 0)
        self.assertEqual(stats.core_works[0].work_type, "article")

    async def test_same_title_different_doi_collapses_to_archival_core(self) -> None:
        article = make_work(
            work_id="https://openalex.org/W1",
            title="Formal analysis of orchestrated service reliability",
            work_type="article",
            doi="10.1000/article",
            track="core",
        )
        preprint = make_work(
            work_id="https://openalex.org/W2",
            title="Formal analysis of orchestrated service reliability",
            work_type="preprint",
            doi="10.1000/preprint",
            track="preprint_watchlist",
        )

        stats = _deduplicate_tracks([article], [preprint])
        self.assertEqual(len(stats.core_works), 1)
        self.assertEqual(len(stats.watchlist_works), 0)
        self.assertEqual(stats.core_works[0].work_type, "article")

    async def test_exact_title_resolution_removes_preprint_from_watchlist(self) -> None:
        ctx = self._context()
        article = make_work(
            work_id="https://openalex.org/W10",
            title="Formal analysis of orchestrated service reliability",
            work_type="article",
            doi="10.1000/article",
            track="core",
        )
        preprint = make_work(
            work_id="https://openalex.org/W11",
            title="Formal analysis of orchestrated service reliability",
            work_type="preprint",
            doi="10.1000/preprint",
            track="preprint_watchlist",
        )

        result = await _resolve_preprints_against_core(
            ctx,
            object(),  # type: ignore[arg-type]
            core_works=[article],
            watchlist_works=[preprint],
        )
        self.assertEqual(result.resolved_to_existing_core, 1)
        self.assertEqual(result.resolved_to_new_archival, 0)
        self.assertEqual(len(result.watchlist_works), 0)
        self.assertEqual(len(result.core_works), 1)
        self.assertEqual(result.resolution_rows[0]["method"], "exact_title")

    async def test_title_search_resolution_adds_archival_record_not_preprint(self) -> None:
        ctx = self._context()
        preprint = make_work(
            work_id="https://openalex.org/W20",
            title="Formal analysis of orchestrated service reliability",
            work_type="preprint",
            doi="10.1000/preprint",
            track="preprint_watchlist",
        )
        archival_match = {
            "id": "https://openalex.org/W21",
            "title": "Formal analysis of orchestrated service reliability",
            "type": "article",
            "doi": "10.1000/article",
            "publication_date": "2024-01-01",
            "publication_year": 2024,
            "cited_by_count": 12,
            "abstract_inverted_index": {"formal": [0], "analysis": [1], "distributed": [2], "service": [3], "reliability": [4]},
        }

        with patch("scisieve.pipeline._search_archival_match_by_title", new=AsyncMock(return_value=(archival_match, 0.97))):
            result = await _resolve_preprints_against_core(
                ctx,
                object(),  # type: ignore[arg-type]
                core_works=[],
                watchlist_works=[preprint],
            )

        self.assertEqual(result.resolved_to_existing_core, 0)
        self.assertEqual(result.resolved_to_new_archival, 1)
        self.assertEqual(len(result.watchlist_works), 0)
        self.assertEqual(len(result.core_works), 1)
        self.assertEqual(result.core_works[0].work_type, "article")
        self.assertTrue(result.core_works[0].is_in_core_corpus)
        self.assertEqual(result.resolution_rows[0]["action"], "added_new_archival_core")


if __name__ == "__main__":
    unittest.main()
