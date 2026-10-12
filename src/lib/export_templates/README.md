# Improsa execution export

`improsa_layout.json` contains only formatting extracted from the owner-supplied
legacy XLS sample, SHA-256
`1d58e45dc11cb44b22d524aa782383bf3f9dd7ff95b53b53d60f34eb1501e983`.
It contains no account numbers, execution data or formulas. The independent
reference fingerprints in `tests/fixtures/improsa_reference_fingerprints.json`
verify styles, dimensions, headers and merges across multiple exported rows.

## Owner-confirmed conventions (2026-10-12)

- Contract number: `IBOrderID`; never substitute account ID or instrument Conid.
- FACIAL: IBKR Quantity, preserving units and sign.
- Gross price: IBKR Price.
- Transaction value: positive gross `abs(Quantity * Price)`, following the
  original sample's product; NetCash, commissions and accruals do not alter it.
- Currency: USD is written as `dolares`. Other currencies fail explicitly until
  an authoritative mapping is provided.
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

The original legacy XLS sheet, all header/data cell styles, column widths,
row heights and each execution's merged contract-number area are preserved.
Rows are selected by ExecID; similar executions remain separate. No email is
sent by this workflow.

Run `./venv/bin/python -m pytest tests/test_trade_ticket_export.py -q` from the
API repository. This runs the complete acceptance fixture in isolation with
external DB/Flex/Gmail boundaries mocked and actual Flask/JWT/export code.
The owner's AGM completion rule accepts passing local validation without
waiting for deployment; this does not establish that deployment occurred.
