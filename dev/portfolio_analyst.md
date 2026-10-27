# Portfolio Analyst development script

From `agm-api`:

```sh
PYTHONDONTWRITEBYTECODE=1 venv/bin/python -B dev/portfolio_analyst.py
```

Defaults: account `U20566763`, verified account master `F10740574`, username
`aguiagm2024`, period `1Y`. The PDF is written privately (0600) to an ignored
`output/pdf/` directory with a Costa Rica timestamp. Existing files are refused.
Use `--output`, `--account`, `--master`, `--credential`, `--period`, `--ip`,
`--reference`, or `--sso-master` to select explicitly different inputs.

## Authentication

The script reads the existing `IBKR_ACCOUNT_MANAGEMENT_PRIVATE_KEY` through
Secret Manager and application-default Google credentials. It does not start
Flask, initialize the database, load `.env`, print tokens or save API payloads.

Account Management uses the selected master credential set (FA2 for this master).
The username's SSO is provisioned under the configured FD2 context
(`--sso-master I6413690`), as in the existing Trading API connector. These are
different concepts: SSO provisioning context does not change the target account's
master. Live October 27 verification observed FA2 OAuth success, FA2 SSO 403,
FD2 SSO success, FA Account Details 200 with exact account/master, and Trading
portfolio metadata with that same master. The script requires those identity
matches; it never falls back to another account.

SSO and outer-session portfolio reads do not call brokerage initialization with
`compete:true` or broker logout. There are no order, transfer or account-update
paths. IP detection uses the existing connector's public-IP service unless
`--ip` is supplied. Requests have timeouts and no automatic retry. Repeated PA
queries can hit IBKR pacing limits (historically one request per 15 minutes per
PA endpoint); a 429 is an error, not a reason to replay automatically.

## Sources and output boundaries

- Account Management Details: exact account/master/base-currency verification.
- `/pa/performance`: requested-period daily cumulative returns, daily NAV and
  monthly period returns. Dates, currencies, identities and array lengths are
  validated; returned TWR/MWR is displayed, never relabeled.
- `/pa/allperiods`: available short-period return comparisons, scoped in memory
  to the requested account. No assumption that it includes since inception.
- `/pa/allocation`: instrument and asset-class allocation. The actual live
  first response had realtime=false; the final run had realtime=true. Neither
  supplied an as-of date. Each PDF discloses its actual metadata.
- Portfolio accounts bootstrap, account metadata, summary, ledger and all
  position pages. These are current snapshots, separate from PA history.
- `/trsrv/secdef`: actual company/bond names, ETF classification, sectors,
  principal units, ratings, maturity dates and contractual coupon schedules.

Reference: the owner-supplied 12-page official PDF. The optional reference file
contributes its cover artwork and embedded DroidSans glyphs only, never historical
balances, charts or customer data. Missing TTF metadata is reconstructed in memory
from the PDF's own Identity-H Unicode map. Remaining PDF text, charts and tables
are generated as vectors. Without a reference, built-in fonts and a basic cover
are used instead; that mode is not an exact visual match.

The fee-detail row is omitted as requested, but actual NAV and provider returns
are unchanged. The output remains net as supplied; costs are not added back.

Bond position units are converted to face using the returned principalValue;
cost basis uses original API quantity times avgCost. Supported fixed bullet
coupon schedules are scaled from the returned redemption notional to that face
amount. Cash flows are contractual estimates over the next three years, without
forecasting early calls, taxes, defaults or holding changes. Floating, amortizing
or ambiguous schedules remain unsupported. Rating views use current issue
ratings; computed maturity bands and projections are not certified equivalent
to broker PDF calculations. ETF/stock dividends are unavailable, not zero.

## Current limits, verified October 27, 2026

Actual PA response: 262 daily points, October 27, 2025–October 27, 2026, TWR;
13 monthly entries including partial boundary months; 16 current positions;
eight bonds with supported contractual schedules. The official reference uses
since-inception MWR through September 28 and is therefore not numerically
comparable to this request. The API's monthly values are shown directly; the
script does not invent full-quarter/year totals from partial months.

The deprecated `/pa/summary` exploratory read returned HTTP 500 and is not used
or retried by the script. No returned source supplies the complete period
NAV-change bridge, top/bottom per-security period-return rankings, duration/YTM,
ETF/stock dividend forecasts or account-level deposits/withdrawals.
`/pa/transactions` is contract-specific and cannot certify that account cash
ledger. The PDF marks those gaps explicitly and identifies itself as an AGM
reconstruction. It is not a complete or identical official PortfolioAnalyst PDF.

Validation uses the real responses retained in memory, renders the resulting
PDF, and checks account identity, currency, every series/holding, coupon scaling,
pagination, font mapping and fee-detail omission. Offline diagnostic probes and
page renders are not persisted. The requested PDF is the sole generated artifact.
No GitHub posts, deployment, application routes or broker writes are involved.

Final standalone command passed with exit 0 at approximately 11:01 Costa Rica:

```sh
PYTHONDONTWRITEBYTECODE=1 venv/bin/python -B dev/portfolio_analyst.py --output output/pdf/U20566763_PortfolioAnalyst_2026-10-27.pdf
```

This fresh run made 13 bounded requests after the PA pacing interval. The saved
PDF has 11 landscape pages, selectable vector text/charts, exact account/master
identity, TWR labeling and no fee-detail row. Every page's text bounds and 0600
permissions passed; the saved PDF was rendered and visually inspected. Actual
position cost/P&L, quote/face units and the eight coupon schedules were separately
reconciled using the first real response set in memory. Wrong-master, currency,
missing/nonfinite NAV and unknown-measure guards rejected in-memory variants.
No live API replay was used for those regression probes. Some original sections
and the original MWR/inception coverage remain unavailable, as listed above.
