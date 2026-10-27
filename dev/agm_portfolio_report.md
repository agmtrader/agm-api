# AGM portfolio report

Generate the independently designed AGM report from actual IBKR responses:

```sh
PYTHONDONTWRITEBYTECODE=1 venv/bin/python -B dev/agm_portfolio_report.py
```

The defaults remain account `U20566763`, account master `F10740574`, username
`aguiagm2024`, period `1Y`, and the existing verified FD2 SSO provisioning context.
Use `--output` to select a specific PDF destination. Credentials and responses
remain in memory. Files are created with 0600 permissions; existing PDFs are not
overwritten by the command. Generated PDFs live under ignored `output/pdf/`.

## Independence from the broker-style version

The previous `dev/portfolio_analyst.py` script is preserved byte-for-byte. This
entry point imports its read-only data client, validation and bond data adapter;
it does not invoke its PDF renderer or access the original broker PDF. The new
design uses only AGM's existing logo, its navy/orange/white palette, separately
licensed Rubik font assets and original report wording. It has no IBKR cover
artwork, extracted broker font, copied disclosure paragraphs or PortfolioAnalyst
product title. API access, data-distribution rights and regulatory obligations
still depend on AGM's actual agreements and applicable requirements; the design
is not a legal-clearance certification.

The bundled unmodified Rubik Regular and Medium fonts are from the Google Fonts
Rubik repository, pinned at e337a5f69a9bea30e58d05bd40184d79cc099628,
`old/version-2/fonts/ttf/`. The full SIL Open Font License is retained beside
them in `report_fonts/OFL.txt`.

## What the reader sees

An AGM summary, period performance and monthly returns, current holdings,
allocation/maturity bands, supported bond-income estimates, contractual payment
detail, and original methods/coverage notes. Data coverage is explicitly partial.
Broker-reported figures and AGM calculations are identified separately. Current
holdings and historical performance are different snapshots. Currencies remain
explicit; unavailable data is not converted to zero.

The actual API returns TWR over the selected one-year period, not the original
report's since-inception MWR. Quantity/face and cost conversions, maturity bands,
coupon scaling, annual/remaining coupon estimates and current yield retain the
documented calculations from the data adapter. Projections assume unchanged
positions and contractual payments and do not predict early calls, defaults or
taxes. Unsupported schedules are excluded, with their supported count visible.

Fee-detail lines remain omitted; actual NAV and broker returns are unaltered,
without adding back costs. Full NAV attribution, security-level period rankings,
duration/YTM, equity-dividend forecasts and the complete deposits/withdrawals
ledger remain unavailable. Source scope, assumptions and limitations are included
in the PDF rather than replaced with borrowed broker disclosures.

Authentication/read behavior is documented in `portfolio_analyst.md`. This script
does not submit broker instructions, compete for a trading session, post to
GitHub or deploy an application. Repeated PA queries must respect broker pacing;
there is no automatic retry. Offline layout checks reuse real responses in memory.

## October 27 verification

The actual entry point authenticated, verified the target account/master, made
13 bounded requests and generated an eight-page PDF from 262 daily NAV/return
points, 13 monthly entries and 16 current positions. All eight supported bond
schedules are included. Every page was rendered and inspected; text bounds,
account/measure labels, absence of copied broker disclosures, licensed Rubik
fonts and 0600 permissions were checked. A wrong-master variant was rejected.
Layout refinement reused the real response bundle without further broker reads.

Preserved broker-style script SHA-256:
`c0a4e936269679b1ba5f2110c0f3e2e605c608cf85bc78c371a2fa38c2a0bd07`.
The original broker-style script and its renderer were not edited.
