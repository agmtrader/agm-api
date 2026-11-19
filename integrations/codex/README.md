# AGM API connector for Codex

The canonical AGM marketplace is `~/Library/Mobile Documents/com~apple~CloudDocs/Codex/Marketplaces/agm` in iCloud Drive. The AGM plugin includes an `agm-api` skill and a local MCP server for `https://api.agmtechnology.com`. Its catalog intersects the generated API documentation with a list of reviewed GET routes. AGM enforces the signed-in user's existing scopes and ownership rules.

Sign in from Terminal:

```sh
"$HOME/Library/Mobile Documents/com~apple~CloudDocs/Codex/Marketplaces/agm/plugins/agm/mcp/launch" login
```

The command prompts for email and a hidden password and saves the short-lived JWT in a user-only local state directory. Repeat login when the session expires. Run the same command with `logout` to remove the JWT. Enter credentials only in Terminal.

- `find_endpoints` searches the available GET routes.
- `describe_endpoint` shows the exact route template and documented parameters.
- `read_api` retrieves an allowed GET route. Supply template values in `path_parameters`. Array query values repeat the key. Use `save_response` for large results such as the bond report. Inline responses are capped at 256 KiB and private local downloads at 128 MiB.

The AGM API skill describes how to batch large reports with `read_api` and verify the coverage of each response.

Both write methods and GET routes with operational side effects are excluded. Newly documented routes require review before they are added to the allowed set.

From the `agm-api` checkout, refresh the canonical iCloud package and the local Codex installation:

```sh
python3 -B integrations/codex/install_plugin.py
codex plugin marketplace add "$HOME/Library/Mobile Documents/com~apple~CloudDocs/Codex/Marketplaces/agm"
codex plugin add agm@agm
```

The installer requires Python 3.10+, preserves the other AGM skills and bundles the local generated API docs. Open a new Codex chat to load the updated connector.
