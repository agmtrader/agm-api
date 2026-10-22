# Market Data ETL Cloud Run runner

Market uses the working clients execution pattern with its own job, staging and
lock. Clients runner configuration, schedule and accepted coverage policy remain
unchanged. This migration preserves October 20 extractor provenance and verified
stable-report publication. API #86 owns migration/acceptance; #299 retains the
separate five-workflow post-deployment maintenance review.

## Execution and result

- `agm-market-etl`, `agm-datalake/us-central1`, one task/parallelism one, one CPU,
  2 GiB, 60-minute timeout, no automatic task or whole-job retries.
- `python -m src.jobs.market_etl --storage-bucket agm-datalake-etl-runs
  --output-dir /app/cache/market-run` runs the existing pipeline without Flask.
- `ETL_BATCH_FOLDER_ID` must identify a dedicated market folder, separate from
  clients staging and shared/manual inputs. Nonempty startup staging fails closed
  so interrupted output is preserved for review.
- `locks/market-etl.json` uses generation-zero acquisition and owned-generation
  release. Concurrent executions fail before pipeline writes. Do not automatically
  remove a crashed execution's lock or clear its staging; verify terminal execution
  and publication state before an operator conditionally remediates it.
- Durable private `runs/<execution>/overview.json` records raw/operational outcomes,
  exact source IDs, stages, start/completion time and lock-release errors. Missing
  overview, failed publication/release, partial stages and inconsistent/missing
  source results cause failure. Market never inherits clients coverage exceptions.
- API `GET`/`POST /etl/market_data` dispatches once using its Google runtime identity,
  returning 202 with operation/status URL. Existing JWT/scope enforcement applies.
  `GET /etl/market_data?operation=<id>` polls only an operation identifying the fixed
  market job. It reports job completion, not independently certified pipeline
  success; the private overview supplies stage evidence. An uncertain launch must
  be inspected before another dispatch. Clients' legacy route remains separate.

## Deployment

Existing jobs: `python3 dev/sync_market_job.py --source-ref <commit>` plans a
source-only update; `--apply` publishes it without executing ETL. The workflow
runs this synchronizer with its exact `GITHUB_SHA` before dispatch. It preserves
the existing immutable image, staging, identity, VPC, resources and other policy,
pins the connector-inclusive source bundle by generation/SHA-256, verifies uploaded
bytes, uses the existing etag and checks configuration readback. Active executions
or a retained lock block source replacement. Synchronization failure prevents
dispatch. `deploy_market_job.py --update` delegates to the same path, fixing the
old unsupported Jobs.patch `updateMask` request. Initial provisioning below
remains separate.

`dev/deploy_market_job.py --staging-folder <verified-market-folder>` is read-only
planning. Add `--apply` to upload the reproducible exact-path-allowlisted source
bundle and provision the job; add `--update` only for an existing job. Provisioning
does not execute ETL. The script uses the current immutable API runtime image,
existing service identity/VPC, `DEV_MODE=false` and a generation-pinned SHA-256
artifact in the existing private ETL bucket. No secret values or raw broker data
are part of the artifact. API route deployment requires the normal API release.

Provision/verify the job before publishing the job-based scheduled workflow and
the dispatch route. Confirm the caller identity has job invocation/execution-read
permissions and overview read access; do not broaden credentials speculatively.
Run daily at 13:37 UTC / 07:37 CST (Costa Rica), alongside clients ETL; preserve existing
notification recipients. The workflow polls its exact execution and requires
Cloud Run terminal success plus a matching validated market overview. There is no
HTTP fallback, no ETL replay, and no notification on mere dispatch acceptance.

## Verification

Local checks exercise the actual runner/pipeline DataFrame-source handling, real
Flask JWT/scope/dispatch boundary, and actual Actions shell with in-memory providers.
Failures cover stale/missing sources, partial stages, concurrent lock rejection,
nonempty/shared staging, lock release/publication failure, failed execution and
wrong-execution overviews. Publication checks retain the existing stable reports
under staged-upload/checksum/update failure. Fixture checks do not prove hosted
IBKR/Drive, IAM or the first real scheduled run.

Record provisioned folder, bundle generation/digest, runtime image, actual execution
and first scheduled acceptance below before claiming the cloud migration complete.

## Provisioning and local evidence — October 20, 2026

- Dedicated market staging: `1SxEb1sQ5Tq8nmCSK9joZZaGPA_Cz1HBw`, under the same verified shared-drive parent as clients staging. Shared-drive creation requires `supportsAllDrives=true`; the generic helper omitted it, so no generic connector change was included.
- Provisioned job source bundle: `artifacts/market-5549b4b0c47eff7a8b912c6f8372b09341c62776a26a438616d9651ba212db81.zip`, generation `1792520366388276`.
- SHA-256: `5549b4b0c47eff7a8b912c6f8372b09341c62776a26a438616d9651ba212db81`.
- Runtime image: `us-central1-docker.pkg.dev/agm-datalake/cloud-run-source-deploy/agm-api@sha256:a714c24a939c2d2b3ea6c811a5e2bc8f1e9148b69c6ec848846e2498d0c0e937`.
- Source artifact, runtime image, stage and zero retries independently read back from the created job. Existing API runtime identity already has `roles/run.admin` and `roles/storage.objectAdmin`; no IAM changes made.
- Controlled direct dispatch returned 202 and operation `4e8377b1-a698-4cda-81d6-9010d26356b1`, selecting `agm-market-etl-vb9g8`. This dispatch uses the actual connector with local ADC, not hosted Flask authentication. No workflow email was triggered. Execution acceptance passed: Cloud Run completed successfully at 18:26:28.166399 UTC (12:26:28 Costa Rica), in 6m19.65s; raw and operational status success, four successful extracts, successful backup, four successful transforms. Independent GCS read returned the exact overview; the market lock returned HTTP 404 and the isolated stage was empty. All four canonical resources exist uniquely with nonzero size/MD5 and modification times from this execution. The actual Actions validator accepts the real Cloud Run v1 execution and durable overview. The read-only operation poll reports completed for that same execution.
- Local affected checks: 74 passed. Broader API/accounting/reporting checks: 270 passed plus 75 subtests. Final scoped checks protect manual dispatch/poll failures and response caching. Embedded workflow Bash, YAML, AST/compile and diff checks pass.
- Compliance process 05 and PROC-005 are updated in the existing dirty compliance worktree; the other 36 register rows are unchanged from the pre-task working copy. No account/contact/schema changes, clients rerun or client baseline changes.

The controlled cloud execution establishes runtime/provider/publication acceptance.
The first scheduled GitHub invocation and actual notification delivery remain
separate observations; no workflow email was triggered by this controlled test.
Source publication starts the normal Compile Check and gated API build/release.
Hosted authenticated manual-route acceptance is not claimed from local Flask tests
or from the direct local-ADC dispatch.

## Session-loss recovery and equity coverage — October 22, 2026

Execution `agm-market-etl-w46wk` initially established its brokerage sessions, then
lost readiness during bonds and stocks. Those extracts failed; the missing-source
transform errors correctly preserved their previous reports. UST and ETF sources
succeeded. The specific cause of IBKR losing readiness is not established.

Market extracts now permit one logged recovery across allowlisted read requests
per source. A known expired-session 401 obtains fresh SSO/init/accounts; the exact
500 accounts prerequisite repeats accounts bootstrap. Only the failed read is
repeated. Unknown errors and 403s do not qualify; orders cannot enter this path.
Persistent session errors abort symbol resolution/history rather than being
counted as isolated unavailable symbols. `session_recoveries` remains in the
private overview and a nonzero count is shown in the workflow summary. Existing
strict source provenance, publication checks and zero whole-job retries remain.

Brokerage initialization, including a recovered SSO session, waits IBKR's documented
two seconds before the accounts bootstrap. Known session errors retain sanitized
provider diagnostics and authentication-status flags in private `session_failures`,
including the final error after the recovery budget is exhausted. A diagnostic
request failure stays unavailable and cannot replace the original read error.

The previously deployed background stock/ETF historical enrichment is preserved
in committed source: `historical_yields: true`, `DEV_MODE=false`, sequential daily
history pages with 1.25-second pacing, including recovery reads. `Current Yield`
is the mean of five annual price returns, not dividend/total return. Unavailable
history remains blank; zero/negative finite results remain valid. Extract metadata
records requested/populated/unavailable counts. Successful pipeline status does
not certify complete history coverage.

## Recovery acceptance — October 22, 2026

Source `0e39c05` passed [GitHub Action 39843963070](https://github.com/agmtrader/agm-api/actions/runs/39843963070).
All four extracts, backup and all four transforms succeeded. Independent downloads
verified current-run source provenance, preserved canonical report IDs, row counts
and Drive checksums. Published stock/ETF prices and return values match their exact
source files. The execution lock was released and market staging was empty.
Stock historical coverage was 99/99; ETF coverage was 60/61, with one unavailable
history retained blank. No session recovery was needed in this accepted run. The
existing success-notification action passed; recipient delivery was not separately
verified. The 95 affected local regressions also passed.

The preceding candidate run correctly failed on a second ETF session error after
one successful recovery. A subsequent read-only ETF diagnostic passed with the
documented initialization settlement interval before this complete retest. The
observed improvement does not establish why IBKR invalidated the earlier sessions.
Future persistent session failures remain failures; no retry budget was increased.
The broader first automatic-run/manual Clients dispatch scope remains in API #86.

## ETF universe change — October 22, 2026

At the owner's request, VWRA is removed from the configured ETF universe after
its older-history request remained unavailable. Subsequent extracts request the
remaining 60 ETFs. The next successful market publication removes its row from
the canonical ETF report; historical backup files remain unchanged. The 60/61
coverage above describes the preceding run before this exclusion.

The source change passed 26 focused Market runner/source checks. GitHub Action
[39852428335](https://github.com/agmtrader/agm-api/actions/runs/39852428335) deployed
`c03cdec` and executed the updated job. ETF extraction and transformation passed
with 60/60 historical returns populated. The complete workflow failed because
the Treasury and stock extracts encountered persistent IBKR session errors;
their transforms preserved previous reports. This is a failed full-pipeline
retest, despite the successful ETF exclusion. No retry budget was increased.
