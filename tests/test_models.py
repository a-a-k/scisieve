import unittest

from models import CloudContext, ResilienceParadigm, infer_cloud_context, infer_resilience_paradigm, infer_topic_labels


class ModelInferenceTests(unittest.TestCase):
    def test_general_cloud_requires_explicit_anchor(self) -> None:
        text = "We evaluate resilience in cloud computing services deployed in a data center."
        self.assertEqual(infer_cloud_context(text), CloudContext.GENERAL_CLOUD)

    def test_non_computing_cloud_language_returns_unknown(self) -> None:
        text = "Urban flood resilience assessment method based on cloud model and game theory."
        self.assertEqual(infer_cloud_context(text), CloudContext.UNKNOWN)

    def test_formal_signal_maps_to_formal(self) -> None:
        text = "This study applies formal verification and model checking to cloud services."
        self.assertEqual(infer_resilience_paradigm(text), ResilienceParadigm.FORMAL)

    def test_generic_text_without_method_signal_returns_unknown(self) -> None:
        text = "This paper discusses resilience in distributed service operations."
        self.assertEqual(infer_resilience_paradigm(text), ResilienceParadigm.UNKNOWN)

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
