import datetime
import unittest
from unittest.mock import AsyncMock, patch

from main import (
    _build_prisma_counts,
    _deduplicate_tracks,
    _resolve_end_date,
    _resolve_preprints_against_core,
    _to_research_work,
)


def make_abstract_index(text: str) -> dict[str, list[int]]:
    index: dict[str, list[int]] = {}
    for position, token in enumerate(text.split()):
        index.setdefault(token, []).append(position)
    return index


def make_work(
    *,
    work_id: str,
    title: str,
    work_type: str,
    doi: str | None,
    abstract: str = "",
    publication_date: str = "2024-01-01",
    cited_by_count: int = 0,
) -> dict:
    return {
        "id": work_id,
        "title": title,
        "type": work_type,
        "doi": doi,
        "publication_date": publication_date,
        "publication_year": int(publication_date[:4]),
        "cited_by_count": cited_by_count,
        "abstract_inverted_index": make_abstract_index(abstract) if abstract else {},
    }


class DeduplicationTests(unittest.TestCase):
    def test_same_doi_collapses_to_archival_core(self) -> None:
        title = "Fault tolerant cloud service modeling"
        article = _to_research_work(
            make_work(
                work_id="https://openalex.org/W1",
                title=title,
                work_type="article",
                doi="10.1000/example",
                abstract="cloud computing resilience modeling",
            ),
            source="OpenAlex Core Stream",
            track="core",
        )
        preprint = _to_research_work(
            make_work(
                work_id="https://openalex.org/W2",
                title=title,
                work_type="preprint",
                doi="10.1000/example",
                abstract="cloud computing resilience modeling",
            ),
            source="OpenAlex Preprint Stream",
            track="preprint_watchlist",
        )

        stats = _deduplicate_tracks([article], [preprint])
        self.assertEqual(len(stats.core_works), 1)
        self.assertEqual(len(stats.watchlist_works), 0)
        self.assertEqual(stats.core_works[0].work_type, "article")

    def test_same_title_different_doi_collapses_to_archival_core(self) -> None:
        title = "Fault tolerant cloud service modeling"
        article = _to_research_work(
            make_work(
                work_id="https://openalex.org/W1",
                title=title,
                work_type="article",
                doi="10.1000/article",
                abstract="cloud computing resilience modeling",
            ),
            source="OpenAlex Core Stream",
            track="core",
        )
        preprint = _to_research_work(
            make_work(
                work_id="https://openalex.org/W2",
                title=title,
                work_type="preprint",
                doi="10.1000/preprint",
                abstract="cloud computing resilience modeling",
            ),
            source="OpenAlex Preprint Stream",
            track="preprint_watchlist",
        )

        stats = _deduplicate_tracks([article], [preprint])
        self.assertEqual(len(stats.core_works), 1)
        self.assertEqual(len(stats.watchlist_works), 0)
        self.assertEqual(stats.core_works[0].work_type, "article")

    def test_prisma_counts_use_new_stage_names(self) -> None:
        counts = _build_prisma_counts(
            raw_core_retrieved=10,
            raw_preprint_retrieved=6,
            included_topical_core_raw=4,
            included_topical_preprint_raw=3,
            core_after_dedup=4,
            preprint_watchlist_after_dedup=2,
            preprints_resolved_to_existing_core=1,
            preprints_resolved_to_new_archival=1,
            final_export_core=5,
            final_export_preprint_watchlist=1,
        )
        self.assertEqual(
            list(counts.keys()),
            [
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
            ],
        )
        self.assertNotIn("reclassified_to_core", counts)

    def test_dynamic_end_date_uses_current_utc_date(self) -> None:
        fixed_now = datetime.datetime(2026, 4, 11, 12, 0, tzinfo=datetime.timezone.utc)
        self.assertEqual(
            _resolve_end_date("", now_provider=lambda: fixed_now),
            datetime.date(2026, 4, 11),
        )


class ResolutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_exact_title_resolution_removes_preprint_from_watchlist(self) -> None:
        title = "Fault tolerant cloud service modeling"
        article = _to_research_work(
            make_work(
                work_id="https://openalex.org/W10",
                title=title,
                work_type="article",
                doi="10.1000/article",
                abstract="cloud computing resilience modeling",
            ),
            source="OpenAlex Core Stream",
            track="core",
        )
        preprint = _to_research_work(
            make_work(
                work_id="https://openalex.org/W11",
                title=title,
                work_type="preprint",
                doi="10.1000/preprint",
                abstract="cloud computing resilience modeling",
            ),
            source="OpenAlex Preprint Stream",
            track="preprint_watchlist",
        )

        result = await _resolve_preprints_against_core(
            object(),
            core_works=[article],
            watchlist_works=[preprint],
            raw_by_openalex_id={},
            start_date="2020-01-01",
            end_date="2026-04-11",
            resolve_preprints=False,
            similarity_threshold=0.92,
            compiled_groups={},
            compiled_exclusions=[],
        )
        self.assertEqual(result.resolved_to_existing_core, 1)
        self.assertEqual(result.resolved_to_new_archival, 0)
        self.assertEqual(len(result.watchlist_works), 0)
        self.assertEqual(len(result.core_works), 1)
        self.assertEqual(result.resolution_rows[0]["method"], "exact_title")

    async def test_title_search_resolution_adds_archival_record_not_preprint(self) -> None:
        preprint = _to_research_work(
            make_work(
                work_id="https://openalex.org/W20",
                title="Fault tolerant cloud service modeling",
                work_type="preprint",
                doi="10.1000/preprint",
                abstract="cloud computing resilience modeling",
            ),
            source="OpenAlex Preprint Stream",
            track="preprint_watchlist",
        )
        archival_match = make_work(
            work_id="https://openalex.org/W21",
            title="Fault tolerant cloud service modeling",
            work_type="article",
            doi="10.1000/article",
            abstract="cloud computing resilience modeling",
        )

        with patch("main._search_archival_match_by_title", new=AsyncMock(return_value=(archival_match, 0.97))):
            result = await _resolve_preprints_against_core(
                object(),
                core_works=[],
                watchlist_works=[preprint],
                raw_by_openalex_id={},
                start_date="2020-01-01",
                end_date="2026-04-11",
                resolve_preprints=True,
                similarity_threshold=0.92,
                compiled_groups={"construct": []},
                compiled_exclusions=[],
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
