---
name: agm-api
description: Read AGM API reports and records with the AGM connector. Use for client trades, historical and year-to-date reports, corporate and Treasury bond reports, market snapshots, and other reads in the connector catalog.
---

# AGM API

Use `find_endpoints` and `describe_endpoint` to confirm the documented GET route, supported filters, and parameter format. Retrieve data with `read_api`. For corporate bonds use `GET /reporting/rtd`. For U.S. Treasuries use `GET /reporting/ust_bonds`.

## Large and historical reports

1. For broad requests, plan small, nonoverlapping batches before reading. Use the endpoint's documented date filters or pagination. Preserve the other filters in every call. If no supported partition is available, use the documented report and disclose any size limit you hit.
2. Call `read_api` separately for each batch with `save_response: true` for large row sets. Run at most two independent requests at a time. With cursor pagination, inspect each page before requesting the next.
3. If a batch times out or exceeds the download limit, split that batch into smaller batches if the documented filters allow it. Keep successful results. A sign-in error requires session refresh before further API calls.
4. Read the private response files locally and verify row counts, dates, and coverage. Distinguish periods with rows, periods without rows, and failed reads. A successful request with no rows does not establish zero activity. A period with rows can be a partial snapshot. If the source timestamp is unavailable, use separate publication evidence or state that freshness could not be verified.

## Client trades

Use `GET /reporting/trades` with explicit `years` and `months`. Use one year and at most two months per call. For a past year, cover all twelve months unless the user narrows the scope. For the current year, cover January through the current month in the user's timezone. The endpoint expects a single comma-separated string for `months`.

```json
{"method":"GET","path":"/reporting/trades","query":{"years":"2026","months":"01,02"},"save_response":true}
```

Continue with `03,04`, `05,06` and the rest of the requested period. Split a two-month batch into single-month calls if it times out or exceeds the download limit. Check the returned `Year` and `Month` against every requested month. Use `TradeDate` to report the last trade date actually present. Count only rows where `LevelOfDetail == "EXECUTION"` when reporting executions, since the report also includes order, lot, and summary rows.

## Sign-in

When the API requires sign-in, tell the user to run this in Terminal.

```sh
"$HOME/Library/Mobile Documents/com~apple~CloudDocs/Codex/Marketplaces/agm/plugins/agm/mcp/launch" login
```

Keep AGM credentials out of chat. The connector exposes only reviewed read routes. Requests to change records, send email, or trigger ETL are outside this connector's capabilities.

The AGM marketplace source is `~/Library/Mobile Documents/com~apple~CloudDocs/Codex/Marketplaces/agm` in iCloud Drive. Apply marketplace changes there and reinstall the plugin to refresh Codex's local cache.
