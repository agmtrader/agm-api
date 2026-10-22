# Improsa execution export

## Required output and remaining work (2026-10-22)

Use the original Improsa email and its `04-09 FM.xls` and Vestima PDF attachments
as the reference. Output must be a real formatted XLS: retain all 28 columns,
original highlighted fills, fonts, widths, date formats and merged cells. The
owner-approved exception is four displayed decimals for quoted price in column K.
The owner removed the temporary ACOBO CSV path. The API rejects CSV requests;
ACOBO retains its existing email flow and now offers a separate Download
Improsa Excel (.xls) action; Improsa retains its original XLS download.

Remaining work is tracked in API #87: resolve the missing production contract
identifier, verify a newly generated application download using ACOBO's bond
cash convention, and obtain source/recipient acceptance.
The ACOBO sample
shortcut tracked in Dashboard #17 uses this same styled XLS exporter and still
enforces its other required fields. ACOBO sends `review_sample: true`; the server
checks query ownership/scopes and allows a missing contract to remain blank in
review XLS for any authorized ticket. Shared assignment rows and ticket names
do not restrict this owner-approved testing mode. It never substitutes account or
instrument IDs. The download is named `improsa-review-sample.xls`; normal Improsa
XLS still requires the contract and remains `improsa-trade-ticket.xls`.
Review the completed workbook against the attachments and obtain recipient
acceptance. Preserve approved USD/GBP labels; do not silently convert amounts.

`improsa_layout.json` contains only formatting extracted from the owner-supplied
legacy XLS sample, SHA-256
`1d58e45dc11cb44b22d524aa782383bf3f9dd7ff95b53b53d60f34eb1501e983`.
It contains no account numbers, execution data or formulas. The independent
reference fingerprints historically verified styles, dimensions, headers and
merges. The current focused acceptance fixture checks the layout profile while
allowing the owner-approved price precision change.

## Owner-confirmed conventions (2026-10-12)

- Contract number: `IBOrderID`; never substitute account ID or instrument Conid.
- FACIAL: IBKR Quantity, preserving units and sign.
- Gross price: IBKR Price.
- Transaction value: for IBKR AssetClass BOND/BILL, use ACOBO's TotalAmount:
  `round(abs(NetCash) + abs(AccruedInterest), 2)` (owner-approved October 22).
  Column M contains positive `abs(AccruedInterest)`. Both inputs are required and
  finite, with explicit zero allowed. Do not infer missing cash from price or
  add commission components a second time. Other asset classes retain
  `abs(Quantity * Price)`. AssetClass is required.
- Quoted Price stays unchanged in column K and displays four decimal places.
- Currency: USD is written as `dolares`. On 2026-10-15 the owner approved GBP
  as `GBP`, preserving source amounts with no FX conversion. Other currencies
  fail explicitly until an authoritative mapping is provided.
- Exchange and market: Improsa output codes `BXV` and `S`.
- Transaction type: BUY -> C, SELL -> V, preferring IBKR Buy/Sell over the
  generic TransactionType field (which can contain ExchTrade).
- Unlabeled column J: numeric zero.
- Maturity: actual source/parsed bond maturity when available, otherwise
  Improsa's 01/01/2099 placeholder. Days to maturity is recalculated from the
  execution's operation date; the sample's stale/inconsistent count is not a
  reusable rule.
- Other optional numeric fields remain blank when absent, not fabricated zero.
- Missing required fields and non-finite numbers reject the workbook with a
  visible HTTP 400 rather than producing an incomplete or invalid export.

The original legacy XLS sheet, header/data styles except quoted-price precision,
column widths, row heights and each execution's merged contract area are preserved.
Rows are selected by ExecID; similar executions remain separate. No email is
sent by this workflow.

## Bond percentage-price correction — 2026-10-20

Historical behavior, superseded for exported bond totals on October 22 below.
The owner authorized dividing the bond/bill principal calculation by 100 after
checking the quote convention. For example, 100,000 face value at 69.4193
produces 69,419.30 principal while column K retains the quote 69.4193.
Accrued interest and commission remain separate exported fields. Local
exporter/Flask regression checks reproduce the earlier 100x amount and pass
after the change; the original fund example remains 5,395 x 14.57 = 78,605.15.
Actual application/download testing is explicitly deferred by the owner until
more executions are available later today. No deployment is claimed.

## Shared ACOBO bond amounts and price precision — 2026-10-22

The owner's generated four-execution workbook confirmed the earlier clean
principal scaling. The owner then requested the same accrued-interest and cash
calculation as the established ACOBO ticket. Column N now uses the shared ACOBO
TotalAmount helper, and column M uses positive accrued interest. Column AB stays
informational; it is not added to the total. ACOBO's existing behavior is retained,
including its round-after-summing consolidated total. Columns K retain stored
quote precision and now display four decimals. Contract policy is unchanged.

Run `./venv/bin/python -m pytest tests/test_improsa_acobo_amounts.py -q` from the
API repository. It isolates external DB/Flex/Gmail doubles while exercising
the real exporter, confirmation generator and Flask/JWT route. Synthetic NetCash
is explicitly identified: the owner's supplied XLS and USD-only bonds snapshot
do not contain the matching broker cash inputs. Passing these checks does not
establish actual application download, deployment or recipient acceptance.
