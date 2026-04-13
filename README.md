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

## Mandatory Preflight: Anchor Validation
Do not start a real review, evidence extraction, or manuscript-facing analysis until anchor validation passes for the topic bundle you plan to use. This is the minimum acceptance gate for a usable retrieval/screening configuration.

Why this matters:
- A green test suite only proves that the software runs; it does not prove that your topic bundle can actually find the papers that define the review scope.
- Query packs, screening terms, snowballing, and rescue rules can all drift recall and precision.
- If anchor validation is weak, the rest of the review will be biased from the start and later manual work will not fix that reliably.

Minimum validation inputs:
- `anchor_benchmark.csv` with positive anchors that must be found for the topic.
- At least one critical tier of anchors, typically `Tier A`, for must-find papers.
- Optional `Tier B` anchors for stretch/secondary coverage.
- Negative sentinels that must never leak into `scholarly_core`.

Recommended validation process:
1. Build or update the private topic bundle first:
   `query_packs.yaml`, `topic_profile.yaml`, `gray_registry.yaml`, `anchor_benchmark.csv`.
2. Run a debug validation cycle on the exact same bundle you intend to use for research:
```bash
python -m scisieve run --config scisieve.private.yaml --profile debug --run-root .scisieve_runs\live_debug
```
3. Inspect the validation artifacts before doing anything else:
   - `coverage_report.md`
   - `coverage_anchor_results.csv`
   - `coverage_failures.csv`
   - `negative_sentinel_report.csv`
   - `top_missing_anchors_by_pack.csv`
   - `screening_title_abstract.csv`
   - `snowball_included.csv`
   - `snowball_excluded.csv`
4. Tune the bundle and rerun until the benchmark is stable.

Minimum acceptance criteria before research starts:
- `Tier A` coverage must be `100%` on the chosen validation benchmark.
- `Negative sentinels in core` must be `0`.
- `Coverage failures` should be `0` for the benchmark you treat as mandatory.
- If you track a stricter corpus-readiness metric, anchors expected to be included should also land in `scholarly_core` or `preprint_watchlist`, not just be retrieved.

Practical interpretation:
- Passing `anchor-check` is not a nice-to-have report. It is the required proof that the current topic bundle is fit for research use.
- If anchor validation fails, fix the bundle first. Do not proceed to extraction, quality appraisal, or release packaging.
- Validation should be rerun after material changes to query packs, screening rules, snowballing logic, or benchmark sets.

## OpenAlex API Key
- If present, SciSieve automatically reads an OpenAlex API key from `SCISIEVE_OPENALEX_API_KEY`.
- As a local file alternative, it also reads `.scisieve_secrets/openalex_api_key.txt`.
- These secret locations are ignored by git and are not required for public/example runs.

## GitHub Packages
SciSieve uses two distribution channels:
- Python source and wheel artifacts are attached to GitHub Releases.
- A runnable container image is published to GitHub Packages via GHCR.

Example:
```bash
docker pull ghcr.io/a-a-k/scisieve:1.0.2
docker run --rm ghcr.io/a-a-k/scisieve:1.0.2 --help
```

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
