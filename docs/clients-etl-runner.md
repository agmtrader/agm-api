# Clients ETL runner

Current completion policy (October 28, 2026): the full Clients batch attempts extract, backup and transform despite captured file/account/stage errors. It exits zero as `completed_with_errors` when those stages have all been attempted, preserving raw failures and stale-data evidence. Fatal runner/infrastructure failures still exit nonzero. This policy supersedes the historical accepted-gap failure rules below.

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

Full refresh defaults: four concurrent immutable bearer-token requests, global five-starts-per-second pacing, bounded connect/read deadlines. A details HTTP 403 triggers at most one additional read with the other configured credential set; other HTTP statuses, transport/authentication errors and identity/master validation failures do not trigger fallback. Each wave checks the credential-scoped token cache for renewal. Unsupported/conflicting/missing masters and malformed IDs are explicit coverage failures; no guessed default credentials or silent skip list. Missing/unsupported masters remain failures before any broker request. Alternate credentials do not replace the expected parent: exact account ID and original master validation still apply. Private outcomes retain credential-master/status attempts; no account/master database writes occur. Previous response master can supply a missing internal master. Prior-only accounts are also refreshed; brand-new broker account discovery is not implemented here.

Success replaces previous details after identity/master validation. Failure retains last-good details and marks `_agm_refresh.status=stale`; old backups without a known successful-fetch time retain null. The companion report covers failures without prior details. Partial details-only diagnostic results exit 1; full-pipeline completion follows the policy below.

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

The source bundle retains its existing accepted account/error baseline unchanged for historical evidence. As of October 28, the baseline no longer gates batch completion: captured file, account, backup and transform errors yield `completed_with_errors` once all three stages have been attempted. Raw failures and stale/absent coverage remain visible. The runner does not declare stale/absent accounts current, bypass identity checks or add baseline entries. Fatal startup/runner failures, failed durable overview publication and failed lock release still produce nonzero exit.

Deploy or update from the code workspace:

```bash
./venv/bin/python dev/deploy_clients_job.py --staging-folder 1EIshGq22oFg-WJ9aHdS37Heh5KaqN2ct --coverage-report dev/ibkr_details_runs/integrated_20261014T205914Z/refresh_report.json --update
```

The clients GitHub workflow is prepared to launch the job, poll that execution to terminal state, download its final overview, and preserve existing notifications. A workflow-only default-branch cutover commit uses `[skip ci]` to avoid triggering an unrelated API rebuild; the cloud execution itself is the acceptance test. First actual scheduled run remains a separate acceptance check. Deferred account discovery/contact reconciliation is API #294; market migration stays in #86.


### Flex report generation pending
When a configured clients Flex report (742588, 734782, 732383 or 794867) returns IBKR error 1019 with “Statement generation in progress”, the runner may complete successfully if backup and transformation succeed and the matching report identifies the existing backup it published. The overview warnings, GitHub summary and notification identify that no new file was generated and name the backup. Raw extraction failure evidence remains visible. A missing or failed fallback, another IBKR error, or a new details failure remains a reported processing error. After all stages have been attempted, the batch completes with `completed_with_errors`.

### Terminal notification details

The workflow runs `.github/scripts/clients_etl_summary.py` against the downloaded terminal overview and Cloud Run execution metadata. The same plain-text summary appears in the Actions run summary and success/failure email. It includes the workflow URL, execution, actual Cloud Run completion time in UTC, operational and raw statuses, extract/transform counts, backup rename/move/clear outcomes, and stage/step/file errors. Missing evidence is labeled unknown or unavailable, rather than reported as zero failures.

IBKR refresh failures are grouped by safe error category and count; account identifiers remain in the private GCS overview. Accepted coverage gaps and retained stale data remain explicit. Flex fallback warnings name the backup used and the original safe error category. Failure, warning and runner-error sections are omitted when they have no entries. Blank lines separate the overview, stage, refresh and nonempty diagnostic categories. Unknown counts and missing-overview diagnostics remain visible; reported account failures with absent outcomes explicitly say their details are unavailable. Formatting does not change job acceptance, retries, scheduling, recipients, or pipeline execution.

## Schedule revised — October 23, 2026

Clients and market ETL workflows are both scheduled daily at 09:07 UTC / 03:07 CST
(Costa Rica, UTC-6), as requested by the owner. Their separate jobs, staging folders
and locks permit simultaneous execution. GitHub may start scheduled workflows late.

## Automatic source synchronization — October 22, 2026

The Clients job is synchronized after successful API deployment and before every
scheduled/manual Clients workflow dispatch by `dev/sync_clients_job.py`. The script
reads the exact committed source, builds a reproducible immutable overlay, verifies
the uploaded bytes, patches the existing job with its etag, waits for completion,
and reads the configuration back. A failed synchronization stops workflow dispatch.
The deployment workflow checks out the triggering compile run's commit and uses
that commit for the image tag, avoiding a newer default-branch checkout race.

The accepted coverage JSON is copied byte-for-byte from the previous checksum- and
generation-verified bundle. New account failures are never added to that baseline.
Staging, service identity, networking, resource limits, timeout, retry policy and
unrelated environment entries are preserved. The base image is the API revision
serving 100% traffic, pinned by digest; the job is labeled with its source commit.
No job is executed by synchronization. The old provisioning command above remains
for initial provisioning or explicitly authorized baseline changes, not routine
source refreshes.

Read-only plan: `python3 dev/sync_clients_job.py --source-ref HEAD`.
Apply configuration only: add `--apply`. Uncommitted local files are excluded.
Successful synchronization proves deployment provenance; ordinary-run outcomes
remain a separate acceptance check.

## Publication and delinked-account correction — October 22, 2026

The open-positions transform publishes `ibkr_open_positions_all.csv` through the
same verified staging and in-place update helper as the final bond/LOT template.
It no longer deletes the full-position resource before a replacement is verified.
Each report is published separately: if template publication fails after the full
report succeeds, the transform remains failed and the previous template survives.
Uncertain updates are not automatically replayed.

An HTTP 200 details response with the exact requested account ID, no master, and
a valid nonfuture `dateDelinked` is recorded as `skipped / account_delinked` with
the broker's date. It does not count as fresh or require expanding the accepted
failure baseline. Prior details remain historical with `_agm_refresh.status=stale`,
their known last-success time, and the delinking reason/date. No synthetic current
details are created when no previous record exists. The summary reports the
delinked count separately without exporting account identifiers. The account
database and its master mappings are not changed.

Wrong account IDs, explicit conflicting masters, unexplained missing masters,
invalid/future delinking dates and all other new failures remain failed account
outcomes and are reported without failing a completed batch.
Only broker-confirmed delinking can use this skipped-account path. The immutable
accepted-coverage file remains unchanged. These corrections require release and
a subsequent real execution before hosted recovery is established.

## Shared scheduled start revised — October 26, 2026

Owner requested both clients and market ETLs daily at 06:00 UTC / 00:00 CST
(midnight Costa Rica, UTC-6), using `0 6 * * *`. This supersedes earlier schedule
times retained in historical evidence. GitHub Actions may start scheduled workflows
late; the configured time is a target.

## Continue processing and report errors — October 28, 2026

Individual manual-input copy failures are recorded and later copies and pipeline
stages continue. Failure to initialize isolated staging or list the source folder
is a runner failure. Existing extract/transform loops continue after each file
error, and the stage loop attempts all three stages even if a stage raises.
A stage status of `partial` or captured `failed` is processing evidence, not an
uncaught runner error. Missing stages or invalid terminal status remain failures.

The workflow requires successful Cloud Run termination and a completed overview;
it accepts `success`, `completed_with_errors`, and the legacy
`completed_with_accepted_coverage_gaps`. The completed notification uses
`COMPLETED WITH ERRORS` when appropriate and retains the existing detailed
error summary, source/backup warnings and fresh/failed/stale counts.
This does not add retries or replay failed work.
