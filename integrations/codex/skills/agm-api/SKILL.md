---
name: agm-api
description: Read AGM API reports and records with the AGM connector. Use for corporate and Treasury bond reports, market snapshots, or other AGM API reads available in the connector catalog.
---

# AGM API

Use the AGM connector's `find_endpoints` and `describe_endpoint` tools to find the documented GET route and its parameters. Use `read_api` to retrieve the data. For corporate bonds use `GET /reporting/rtd`. For U.S. Treasuries use `GET /reporting/ust_bonds`.

For a large report, pass `save_response: true`. Read the private local response file and verify the row count before summarizing it. A report may be a saved snapshot. If the API response lacks a source timestamp, establish freshness from separate publication evidence or say it could not be verified.

When the API requires sign-in, tell the user to run this in Terminal:

```sh
"$HOME/Documents/Codex/agm-marketplace/plugins/agm/mcp/launch" login
```

Keep AGM credentials out of chat. The connector exposes only reviewed read routes. Requests to change records, send email, or trigger ETL are outside this connector's capabilities.

The AGM marketplace source is `~/Documents/Codex/agm-marketplace` in iCloud Drive. Apply marketplace changes there and reinstall the plugin to refresh Codex's local cache.
