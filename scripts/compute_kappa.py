from __future__ import annotations

import argparse
import csv
from collections import Counter
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute Cohen's kappa from two reviewer columns in a CSV file.")
    parser.add_argument("--csv", required=True, help="Path to input CSV.")
    parser.add_argument("--col-a", required=True, help="Reviewer A column name.")
    parser.add_argument("--col-b", required=True, help="Reviewer B column name.")
    parser.add_argument(
        "--case-sensitive",
        action="store_true",
        help="Treat labels as case-sensitive (default: normalize to lowercase).",
    )
    return parser.parse_args()


def _normalize_label(value: Any, *, case_sensitive: bool) -> str:
    label = str(value or "").strip()
    return label if case_sensitive else label.lower()


def _load_pairs(csv_path: str, col_a: str, col_b: str, *, case_sensitive: bool) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if col_a not in (reader.fieldnames or []) or col_b not in (reader.fieldnames or []):
            missing = [name for name in (col_a, col_b) if name not in (reader.fieldnames or [])]
            raise ValueError(f"Missing required column(s): {', '.join(missing)}")
        for row in reader:
            label_a = _normalize_label(row.get(col_a, ""), case_sensitive=case_sensitive)
            label_b = _normalize_label(row.get(col_b, ""), case_sensitive=case_sensitive)
            if not label_a or not label_b:
                continue
            pairs.append((label_a, label_b))
    return pairs


def compute_kappa(pairs: list[tuple[str, str]]) -> dict[str, Any]:
    n = len(pairs)
    if n == 0:
        raise ValueError("No comparable labeled pairs found (both columns must be non-empty per row).")

    agreement = sum(1 for left, right in pairs if left == right)
    observed = agreement / n

    counts_a = Counter(left for left, _ in pairs)
    counts_b = Counter(right for _, right in pairs)
    labels = sorted(set(counts_a) | set(counts_b))

    expected = 0.0
    for label in labels:
        expected += (counts_a[label] / n) * (counts_b[label] / n)

    if expected >= 1.0:
        kappa = 1.0 if observed >= 1.0 else 0.0
    else:
        kappa = (observed - expected) / (1.0 - expected)

    return {
        "n_pairs": n,
        "observed_agreement": observed,
        "expected_agreement": expected,
        "cohens_kappa": kappa,
        "labels": labels,
        "distribution_a": counts_a,
        "distribution_b": counts_b,
    }


def main() -> None:
    args = parse_args()
    pairs = _load_pairs(
        args.csv,
        args.col_a,
        args.col_b,
        case_sensitive=args.case_sensitive,
    )
    result = compute_kappa(pairs)

    print(f"Pairs analyzed: {result['n_pairs']}")
    print(f"Observed agreement: {result['observed_agreement']:.6f}")
    print(f"Expected agreement: {result['expected_agreement']:.6f}")
    print(f"Cohen's kappa: {result['cohens_kappa']:.6f}")
    print("Label distribution (A):")
    for label in result["labels"]:
        print(f"  {label}: {result['distribution_a'].get(label, 0)}")
    print("Label distribution (B):")
    for label in result["labels"]:
        print(f"  {label}: {result['distribution_b'].get(label, 0)}")


if __name__ == "__main__":
    main()
