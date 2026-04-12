import unittest

from protocol import build_screening_text, compile_group_regex, compile_negative_regex, evaluate_protocol, matches_work


class ProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.compiled_groups = compile_group_regex()
        self.compiled_exclusions = compile_negative_regex()

    def test_accepts_microservice_fault_injection_text(self) -> None:
        text = (
            "Microservice resilience testing for cloud computing systems using "
            "fault injection and chaos engineering."
        )
        matched, reason = evaluate_protocol(
            text,
            compiled_groups=self.compiled_groups,
            compiled_exclusions=self.compiled_exclusions,
        )
        self.assertTrue(matched)
        self.assertIsNone(reason)

    def test_accepts_openstack_availability_text(self) -> None:
        text = (
            "Availability modeling in OpenStack IaaS cloud services with a "
            "simulator for resilience assessment under failover."
        )
        matched, reason = evaluate_protocol(
            text,
            compiled_groups=self.compiled_groups,
            compiled_exclusions=self.compiled_exclusions,
        )
        self.assertTrue(matched)
        self.assertIsNone(reason)

    def test_rejects_port_hinterland_container_false_positive(self) -> None:
        text = "Developing a model for measuring the resilience of a port-hinterland container transportation network"
        matched, reason = evaluate_protocol(
            text,
            compiled_groups=self.compiled_groups,
            compiled_exclusions=self.compiled_exclusions,
        )
        self.assertFalse(matched)
        self.assertEqual(reason, "negative_domain_exclusion")

    def test_rejects_cloud_model_false_positive(self) -> None:
        text = "Urban flood resilience assessment method based on cloud model and game theory"
        matched, reason = evaluate_protocol(
            text,
            compiled_groups=self.compiled_groups,
            compiled_exclusions=self.compiled_exclusions,
        )
        self.assertFalse(matched)
        self.assertEqual(reason, "negative_domain_exclusion")

    def test_ignores_openalex_concepts_during_screening(self) -> None:
        work = {
            "title": "Blockchain technology innovations",
            "abstract_inverted_index": {},
            "concepts": [
                {"display_name": "Cloud computing"},
                {"display_name": "Resilience (materials science)"},
                {"display_name": "Simulation"},
            ],
        }
        self.assertEqual(build_screening_text(work), "Blockchain technology innovations")
        self.assertFalse(
            matches_work(
                work,
                compiled_groups=self.compiled_groups,
                compiled_exclusions=self.compiled_exclusions,
            )
        )


if __name__ == "__main__":
    unittest.main()
