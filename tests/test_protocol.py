import unittest

from protocol import build_screening_text, compile_group_regex, compile_negative_regex, evaluate_protocol, matches_work


TEST_GROUPS = {
    "phenomenon": ["reliability", "continuity"],
    "context": ["distributed service", "cluster platform"],
    "method": ["simulation", "fault injection", "formal analysis"],
}

TEST_EXCLUSIONS = ["agriculture", "logistics", "flood model"]


class ProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.compiled_groups = compile_group_regex(TEST_GROUPS)
        self.compiled_exclusions = compile_negative_regex(TEST_EXCLUSIONS)

    def test_accepts_platform_fault_injection_text(self) -> None:
        text = "Fault injection for reliability assessment in a distributed service running on a cluster platform."
        matched, reason = evaluate_protocol(
            text,
            compiled_groups=self.compiled_groups,
            compiled_exclusions=self.compiled_exclusions,
        )
        self.assertTrue(matched)
        self.assertIsNone(reason)

    def test_accepts_formal_analysis_text(self) -> None:
        text = "Formal analysis of continuity properties for distributed service platforms."
        matched, reason = evaluate_protocol(
            text,
            compiled_groups=self.compiled_groups,
            compiled_exclusions=self.compiled_exclusions,
        )
        self.assertTrue(matched)
        self.assertIsNone(reason)

    def test_rejects_logistics_false_positive(self) -> None:
        text = "A simulation study for logistics continuity in regional delivery networks."
        matched, reason = evaluate_protocol(
            text,
            compiled_groups=self.compiled_groups,
            compiled_exclusions=self.compiled_exclusions,
        )
        self.assertFalse(matched)
        self.assertEqual(reason, "negative_domain_exclusion")

    def test_rejects_flood_model_false_positive(self) -> None:
        text = "Reliability assessment using a flood model and simulation for river planning."
        matched, reason = evaluate_protocol(
            text,
            compiled_groups=self.compiled_groups,
            compiled_exclusions=self.compiled_exclusions,
        )
        self.assertFalse(matched)
        self.assertEqual(reason, "negative_domain_exclusion")

    def test_ignores_openalex_concepts_during_screening(self) -> None:
        work = {
            "title": "Distributed service scheduling in retail systems",
            "abstract_inverted_index": {},
            "concepts": [
                {"display_name": "Reliability engineering"},
                {"display_name": "Simulation"},
                {"display_name": "Cluster platform"},
            ],
        }
        self.assertEqual(build_screening_text(work), "Distributed service scheduling in retail systems")
        self.assertFalse(
            matches_work(
                work,
                compiled_groups=self.compiled_groups,
                compiled_exclusions=self.compiled_exclusions,
            )
        )


if __name__ == "__main__":
    unittest.main()
