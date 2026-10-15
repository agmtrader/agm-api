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

## Clients cloud cutover configuration (2026-10-14)

Job `agm-clients-etl` in `agm-datalake/us-central1` uses the existing immutable API runtime image, service account `agm-api@agm-datalake.iam.gserviceaccount.com`, VPC connector `agm-qa-connector` with all-traffic egress, 1 CPU/2 GiB, one task and no automatic retries. No Docker build or API service update is needed: the Python command downloads a generation-pinned GCS code overlay, verifies its SHA-256 and exact path allowlist, then runs the clients module. Source artifact contains only the selected job/ETL modules and the accepted account-gap baseline, not environments or raw client data. Future image/runtime changes require compatibility testing; base image is intentionally pinned until then.

Private bucket `agm-datalake-etl-runs` retains code artifacts and `runs/<execution>/overview.json`. `locks/clients-etl.json` uses generation-match-zero acquisition and generation-conditional release; a concurrent run fails before pipeline writes. A killed process may leave a lock: inspect Cloud Run execution state before an operator conditionally removes it. Never automatically steal a lock from a running execution.

Clients staging folder `1EIshGq22oFg-WJ9aHdS37Heh5KaqN2ct` is separate from the original shared folder. Only manually supplied clients filenames are copied from the existing shared folder; source files are not renamed/moved/deleted by this job. Copied input source IDs/modified times are retained in the overview. Market ETL keeps its existing shared stage and schedule. The legacy API clients endpoint is not the scheduled trigger after cutover; avoid manually invoking it alongside a clients job because it still writes the same reporting resources without the job lock.

The owner accepted current incomplete account coverage. The source bundle embeds the 208 account/error pairs from the verified local report as the explicit accepted baseline. The raw pipeline status and coverage outcomes remain partial/visible, while the job can finish as `completed_with_accepted_coverage_gaps` only when every failure matches that baseline and backup/transform and other extract steps succeed. New gaps, failed publication, failed lock release and other pipeline failures produce nonzero exit. This is not a declaration that stale/absent accounts are current.

Deploy or update from the code workspace:

```bash
./venv/bin/python dev/deploy_clients_job.py --staging-folder 1EIshGq22oFg-WJ9aHdS37Heh5KaqN2ct --coverage-report dev/ibkr_details_runs/integrated_20261014T205914Z/refresh_report.json --update
```

The clients GitHub workflow is prepared to launch the job, poll that execution to terminal state, download its final overview, and preserve existing notifications. A workflow-only default-branch cutover commit uses `[skip ci]` to avoid triggering an unrelated API rebuild; the cloud execution itself is the acceptance test. First actual scheduled run remains a separate acceptance check. Deferred account discovery/contact reconciliation is API #294; market migration stays in #86.


### Flex report generation pending
When a configured clients Flex report (742588, 734782, 732383 or 794867) returns IBKR error 1019 with “Statement generation in progress”, the runner may complete successfully if backup and transformation succeed and the matching report identifies the existing backup it published. The overview warnings, GitHub summary and notification identify that no new file was generated and name the backup. Raw extraction failure evidence remains visible. A missing or failed fallback, another IBKR error, or an unaccepted details failure still fails the run.
