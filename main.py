from __future__ import annotations

import sys

from scisieve.cli import main as scisieve_main


CLI_SUBCOMMANDS = {
    "run",
    "freeze",
    "retrieve",
    "normalize",
    "dedup",
    "anchor-check",
    "screen-ta",
    "snowball",
    "fulltext",
    "extract",
    "quality",
    "gray",
    "release",
    "audit",
}


def main() -> None:
    args = sys.argv[1:]
    if not args or args[0].startswith("-"):
        sys.argv = [sys.argv[0], "run", *args]
    elif args[0] not in CLI_SUBCOMMANDS:
        raise SystemExit(f"Unknown command '{args[0]}'. Use one of: {', '.join(sorted(CLI_SUBCOMMANDS))}")
    scisieve_main()


if __name__ == "__main__":
    main()
