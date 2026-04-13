import unittest

from models import infer_topic_labels, term_in_text


class ModelInferenceTests(unittest.TestCase):
    def test_term_in_text_matches_space_and_hyphen_variants(self) -> None:
        self.assertTrue(term_in_text("fault injection", "Controlled fault-injection experiments were run."))

    def test_dimension_label_requires_explicit_signal(self) -> None:
        labels = infer_topic_labels(
            "We present formal analysis for a cluster platform service.",
            dimensions=[
                {
                    "name": "approach_family",
                    "default": "Unknown",
                    "labels": [
                        {"value": "Formal", "any_of": ["formal analysis", "model checking"]},
                        {"value": "Experimental", "any_of": ["fault injection"]},
                    ],
                }
            ],
        )
        self.assertEqual(labels["approach_family"], "Formal")

    def test_missing_signal_returns_unknown(self) -> None:
        labels = infer_topic_labels(
            "This paper discusses reliability challenges in service operations.",
            dimensions=[
                {
                    "name": "approach_family",
                    "default": "Unknown",
                    "labels": [
                        {"value": "Formal", "any_of": ["formal analysis", "model checking"]},
                    ],
                }
            ],
        )
        self.assertEqual(labels["approach_family"], "Unknown")

    def test_generic_topic_label_inference_uses_supplied_dimensions(self) -> None:
        labels = infer_topic_labels(
            "This paper studies graph neural networks for fraud detection in financial systems.",
            dimensions=[
                {
                    "name": "method_family",
                    "default": "Unknown",
                    "labels": [
                        {"value": "GraphML", "any_of": ["graph neural networks", "gnn"]},
                        {"value": "Rules", "any_of": ["expert rules"]},
                    ],
                },
                {
                    "name": "domain_context",
                    "default": "Unknown",
                    "labels": [
                        {"value": "Finance", "any_of": ["financial systems", "banking"]},
                    ],
                },
            ],
        )
        self.assertEqual(labels["method_family"], "GraphML")
        self.assertEqual(labels["domain_context"], "Finance")


if __name__ == "__main__":
    unittest.main()
