# Clients ETL runner

The runner imports the same `run_pipeline('clients')` used by the HTTP route. It does not start Flask/Gunicorn or call the AGM API. It initializes the existing database connector explicitly. No deployment or scheduler changes have been made.

## Local read-only validation

From the agm-api repository, load the existing environment and run:

```bash
source venv/bin/activate
set -a
source .env
set +a
python -m src.jobs.clients_etl --local-details-only --output-dir dev/ibkr_details_runs/new-unique-run
```

This reads internal accounts and the previous Drive details backup, refreshes the union of those account IDs, and writes `account_details.json`, `refresh_report.json`, and `overview.json` locally. It does not upload, delete or transform Drive files, send emails, or create/update accounts or contacts. `--limit 20` samples the same path and always reports partial; sampled output cannot be published. Files are mode 0600 and ignored by Git/container builds. Existing output files are never overwritten.

Full refresh defaults: four concurrent immutable bearer-token requests, global five-starts-per-second pacing, bounded connect/read deadlines, no automatic retries. Each wave checks the credential-scoped token cache for renewal. Unsupported/conflicting/missing masters and malformed IDs are explicit coverage failures; no default credential fallback and no silent skip list. Previous response master can supply a missing internal master. Prior-only accounts are also refreshed; brand-new broker account discovery is not implemented here.

Success replaces previous details after identity/master validation. Failure retains last-good details and marks `_agm_refresh.status=stale`; old backups without a known successful-fetch time retain null. The companion report covers failures without prior details. Partial results exit 1.

## Full clients pipeline command (writes managed Drive resources)

```bash
python -m src.jobs.clients_etl --output-dir /app/cache/clients-unique-execution
```

This executes the existing extract, backup and transform stages, including the new refresh. It uploads refreshed backup and companion report; the stable transform filters out companion files. Extractor-returned partial outcomes now propagate to the pipeline overview. Other existing extractor behavior is preserved.

Cloud Run Job command override: `python`; arguments: `-m,src.jobs.clients_etl,--output-dir,/app/cache/clients-unique-execution`. Each Cloud Run execution has a fresh container filesystem, but local files are temporary. Use the final status/overview in logs for local diagnosis; durable final overview publication must be configured before cloud cutover. The details companion report is durable in Drive when the extractor completes publication.

## Deployment gates

Do not switch GitHub/API triggers until these are completed:

- Verify candidate image startup, service identity and Secret Manager/Drive/database access, IBKR network access, and full publication acceptance.
- Add durable cross-pipeline execution exclusion or separate batch staging folders. Existing clients and market pipelines share and clear one batch folder; Cloud Run task parallelism does not prevent overlapping executions.
- Persist the full runner overview outside the container even when later stages fail; make workflow notifications depend on terminal pipeline outcome, not dispatch acceptance. Existing HTTP workflows remain unchanged and can still send success for partial HTTP 200.
- Verify the first scheduled clients execution and ensure there is only one active trigger. Market ETL stays unchanged.

Production code changes can also affect the existing HTTP clients route once deployed, so deployment must account for its longer refresh runtime. No container job has been provisioned by this local change.
