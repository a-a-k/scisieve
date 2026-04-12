# SciCrawl

Configurable pipeline for automated SLR/MLR construction. The core engine is topic-driven; the repository currently ships with a bundled cloud resilience/dependability preset. The preferred interface is the package CLI `scicrawl`; `python main.py ...` remains as a legacy compatibility runner for the original preset.

## Install
```bash
python -m pip install -r requirements.txt
```

Optional editable install for the `scicrawl` command:
```bash
python -m pip install -e .
```

## Main CLI
Dry/debug run:
```bash
python -m scicrawl run --config scicrawl.yaml --profile debug
```

Stage-by-stage execution:
```bash
python -m scicrawl freeze scholarly --config scicrawl.yaml --profile debug
python -m scicrawl retrieve scholarly --config scicrawl.yaml --profile debug
python -m scicrawl normalize --config scicrawl.yaml --profile debug
python -m scicrawl dedup --config scicrawl.yaml --profile debug
python -m scicrawl screen-ta --config scicrawl.yaml --profile debug
python -m scicrawl snowball --config scicrawl.yaml --profile debug
python -m scicrawl fulltext --config scicrawl.yaml --profile debug
python -m scicrawl extract --config scicrawl.yaml --profile debug
python -m scicrawl quality --config scicrawl.yaml --profile debug
python -m scicrawl gray --config scicrawl.yaml --profile debug
python -m scicrawl anchor-check --config scicrawl.yaml --profile debug
python -m scicrawl audit --config scicrawl.yaml --profile debug
python -m scicrawl release --config scicrawl.yaml --profile debug
```

## Config Files
- `scicrawl.yaml`: profile settings (`debug`, `csur`) and working directories.
- `topics/cloud_resilience_dependability/topic_profile.yaml`: bundled topic preset that defines screening terms, classifier cues, taxonomy labels, and extraction hints.
- `query_packs.yaml`: scholarly pack definitions for the bundled preset.
- `gray_registry.yaml`: domain-scoped gray-literature families and seed URLs for the bundled preset.

## Generic Topic Configuration
- The execution core is now topic-configurable. Screening terms, negative exclusions, heuristic classifier cues, and label taxonomies are loaded from `paths.topic_profile` in `scicrawl.yaml`.
- A new topic can be introduced by supplying a different topic profile plus matching query packs, gray registry, and benchmark/reference files. The engine stages themselves do not need to be rewritten for that change.
- Metadata exports now include generic label fields: `label_primary_dimension`, `label_primary_value`, `label_secondary_dimension`, and `label_secondary_value`.
- The legacy columns `resilience_paradigm` and `cloud_context` are still emitted for backward compatibility with the bundled cloud preset.

## Output Model
The config-driven pipeline writes working artifacts under `.scicrawl_runs/<profile>/`.

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
- Precision-first screening still uses `title + abstract`; OpenAlex `concepts` are not used for inclusion logic.
- `scholarly_core` contains only archival `article` / `proceedings-article` items.
- Unresolved preprints stay in `preprint_watchlist` and are logged separately.
- Query packs are executed independently, pack-level filters are respected during freeze, and provenance is retained on normalized and retained records.
- Citation expansion uses both OpenAlex and OpenCitations, with separate round logs under `snowball_*.csv`.
- Coverage validation runs against the post-snowball discovered set, not only the initial query-pack retrieval.
- Negative sentinel checks are emitted as explicit reports and must stay at `0` for `scholarly_core`.

## License
This repository is released under the MIT License. See [LICENSE](LICENSE).

## Budget-Aware Resume
- If OpenAlex returns `429 Insufficient budget`, the run now pauses cleanly instead of losing progress.
- `freeze scholarly` checkpoints page-level progress in `freeze_progress.json` and resumes from the saved cursor on the next run.
- `run` writes `resume_state.json`; rerunning the same command with the same `--run-root` continues from the paused stage on the next day.
- Typical resume flow:
```bash
python -m scicrawl run --config scicrawl.yaml --profile debug --run-root .scicrawl_runs\debug_resume
# if budget is exhausted, rerun the same command after the next UTC reset
python -m scicrawl run --config scicrawl.yaml --profile debug --run-root .scicrawl_runs\debug_resume
```

## Legacy Runner
The original baseline script still works:
```bash
python main.py --email your.email@example.org --output-dir output --max-formal-records 50 --max-watchlist-records 50 --resolve-preprints
```

## Tests
```bash
python -m unittest discover -s tests -v
```
