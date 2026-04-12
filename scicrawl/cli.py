from __future__ import annotations

import argparse

from .config import load_resolved_config
from .pipeline import create_context, execute


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scicrawl", description="CSUR-grade automated SLR/MLR pipeline.")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", default="scicrawl.yaml", help="Path to scicrawl YAML config.")
    common.add_argument("--profile", default="debug", help="Profile name from scicrawl.yaml.")
    common.add_argument("--email", default="", help="Optional override for contact email.")
    common.add_argument("--run-root", default="", help="Optional override for the working run directory.")

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("run", parents=[common], help="Run the full pipeline.")

    freeze = subparsers.add_parser("freeze", parents=[common], help="Freeze raw substrate.")
    freeze_sub = freeze.add_subparsers(dest="target", required=True)
    freeze_sub.add_parser("scholarly", parents=[common], help="Freeze scholarly substrate.")

    retrieve = subparsers.add_parser("retrieve", parents=[common], help="Build retrieval union from frozen raw substrate.")
    retrieve_sub = retrieve.add_subparsers(dest="target", required=True)
    retrieve_sub.add_parser("scholarly", parents=[common], help="Retrieve scholarly union from frozen raw substrate.")

    subparsers.add_parser("normalize", parents=[common], help="Normalize frozen scholarly candidates.")
    subparsers.add_parser("dedup", parents=[common], help="Deduplicate and resolve preprints.")
    subparsers.add_parser("anchor-check", parents=[common], help="Run anchor/coverage validation.")
    subparsers.add_parser("screen-ta", parents=[common], help="Run title/abstract screening.")
    subparsers.add_parser("snowball", parents=[common], help="Run iterative citation searching.")
    subparsers.add_parser("fulltext", parents=[common], help="Acquire and parse PDFs.")
    subparsers.add_parser("extract", parents=[common], help="Generate evidence extraction scaffold.")
    subparsers.add_parser("quality", parents=[common], help="Generate quality appraisal scaffold.")
    subparsers.add_parser("gray", parents=[common], help="Run gray-literature lane.")
    subparsers.add_parser("release", parents=[common], help="Build release package.")
    subparsers.add_parser("audit", parents=[common], help="Generate audit artifacts.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    resolved = load_resolved_config(
        config_path=args.config,
        profile_name=args.profile,
        email_override=args.email,
        run_root_override=args.run_root,
    )
    ctx = create_context(resolved)
    execute(args.command, ctx, target=getattr(args, "target", ""))
