# SciSieve
[![Tests](https://github.com/a-a-k/scisieve/actions/workflows/tests.yml/badge.svg)](https://github.com/a-a-k/scisieve/actions/workflows/tests.yml)

Configurable pipeline for automated SLR/MLR construction. The public repository ships only a generic example bundle; real topic-specific bundles are expected to live outside git and be wired in through config paths.

## Install
```bash
python -m pip install -r requirements.txt
```

Optional editable install for the CLI:
```bash
python -m pip install -e .
```

## Main CLI
Dry/debug run:
```bash
python -m scisieve run --config scisieve.yaml --profile debug
```

Stage-by-stage execution:
```bash
python -m scisieve freeze scholarly --config scisieve.yaml --profile debug
python -m scisieve retrieve scholarly --config scisieve.yaml --profile debug
python -m scisieve normalize --config scisieve.yaml --profile debug
python -m scisieve dedup --config scisieve.yaml --profile debug
python -m scisieve screen-ta --config scisieve.yaml --profile debug
python -m scisieve snowball --config scisieve.yaml --profile debug
python -m scisieve fulltext --config scisieve.yaml --profile debug
python -m scisieve extract --config scisieve.yaml --profile debug
python -m scisieve quality --config scisieve.yaml --profile debug
python -m scisieve gray --config scisieve.yaml --profile debug
python -m scisieve anchor-check --config scisieve.yaml --profile debug
python -m scisieve audit --config scisieve.yaml --profile debug
python -m scisieve release --config scisieve.yaml --profile debug
```

`python main.py ...` is kept as a thin compatibility entry point for the same CLI. If no subcommand is provided, it defaults to `run`.

## Public Repo Model
- `scisieve.yaml` points to `examples/example_topic/`, which is intentionally synthetic and generic.
- No real topic bundle, benchmark set, or domain-specific gray registry is bundled in tracked files.
- Topic bundles can be supplied from local ignored paths such as `.scisieve_private/` or from env-expanded absolute paths.
- `load_resolved_config()` expands `~`, `$VAR`, `${VAR}`, and Windows `%VAR%` in bundle paths.

## Private Topic Bundles
Use one of these patterns:

1. Keep a local override config outside git:
```bash
copy scisieve.private.yaml.example scisieve.private.yaml
python -m scisieve run --config scisieve.private.yaml --profile debug
```

2. Point the public config at env-expanded private paths:
```yaml
paths:
  query_packs: ${SCISIEVE_PRIVATE_ROOT}/query_packs.yaml
  gray_registry: ${SCISIEVE_PRIVATE_ROOT}/gray_registry.yaml
  topic_profile: ${SCISIEVE_PRIVATE_ROOT}/topic_profile.yaml
  anchor_benchmark: ${SCISIEVE_PRIVATE_ROOT}/anchor_benchmark.csv
  baseline_metadata: ${SCISIEVE_PRIVATE_ROOT}/baseline_metadata.csv
```

Recommended ignored locations:
- `.scisieve_private/`
- `.scisieve_secrets/`
- `private_topics/`
- `private_reference/`

## OpenAlex API Key
- If present, SciSieve automatically reads an OpenAlex API key from `SCISIEVE_OPENALEX_API_KEY`.
- As a local file alternative, it also reads `.scisieve_secrets/openalex_api_key.txt`.
- These secret locations are ignored by git and are not required for public/example runs.

## Config Files
- `scisieve.yaml`: default generic config with `debug` and `production` profiles.
- `examples/example_topic/topic_profile.yaml`: example screening terms, classifier cues, taxonomy labels, and extraction hints.
- `examples/example_topic/query_packs.yaml`: example scholarly query packs.
- `examples/example_topic/gray_registry.yaml`: example gray-literature families and seeds.
- `examples/example_topic/anchor_benchmark.csv`: synthetic anchor benchmark for tests and smoke runs.
- `examples/example_topic/baseline_metadata.csv`: generic metadata schema header for reference.

## Output Model
The pipeline writes working artifacts under `.scisieve_runs/<profile>/`.

Key outputs:
- `run_manifest.json`
- `retrieved_union.csv`
- `normalized_candidates.csv`
- `metadata.csv`
- `prisma_counts.csv`
- `dedup_summary.csv`
- `coverage_anchor_results.csv`
- `negative_sentinel_report.csv`
- `screening_title_abstract.csv`
- `screening_fulltext.csv`
- `review_queue_borderline.csv`
- `double_screen_sample.csv`
- `fulltext_inventory.csv`
- `evidence_extraction.csv`
- `quality_appraisal.csv`
- `gray_candidates.csv`
- `release_package/`

## Behavior Guarantees
- Screening uses `title + abstract`; OpenAlex `concepts` are not used for inclusion logic.
- `scholarly_core` contains only archival `article` / `proceedings-article` items.
- Unresolved preprints stay in `preprint_watchlist`.
- Query packs are executed independently and retained records preserve query-pack provenance.
- Citation expansion uses OpenAlex and OpenCitations with separate round logs.
- Coverage validation runs against the post-snowball discovered set.
- Negative sentinel checks are emitted as explicit reports and should remain at `0` for `scholarly_core`.

## Budget-Aware Resume
- If OpenAlex returns `429 Insufficient budget`, the run pauses cleanly instead of losing progress.
- `freeze scholarly` checkpoints page-level progress in `freeze_progress.json` and resumes from the saved cursor.
- `run` writes `resume_state.json`; rerunning the same command with the same `--run-root` continues from the paused stage after the next budget reset.

Example:
```bash
python -m scisieve run --config scisieve.yaml --profile debug --run-root .scisieve_runs\debug_resume
```

## Tests
```bash
python -m unittest discover -s tests -v
```

## License
This repository is released under the MIT License. See [LICENSE](LICENSE).
