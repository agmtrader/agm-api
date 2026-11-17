# AGM API connector for Codex

The existing AGM plugin includes a local MCP server for `https://api.agmtechnology.com`. It catalogs method/path pairs from `agm-api/public/static/docs.html`, including parameter notes and dynamic path templates. The AGM API still enforces each account's existing scopes and ownership rules. The connector has no shared credential and needs Python 3.10+ and macOS for the local approval dialog.

In your own Terminal, sign in with your AGM account:

```sh
"$HOME/Library/Mobile Documents/com~apple~CloudDocs/Codex/Marketplaces/agm/plugins/agm/mcp/launch" login
```

The command prompts for email and a hidden password. It calls `POST /token` by default; `login --endpoint /users/login` uses the other documented sign-in route. It saves only the short-lived JWT in a user-only local state directory (`~/.local/state/agm-codex`), outside the shared marketplace. Repeat login when the session expires. Run the same command with `logout` to remove the local JWT. Do not paste a password or access token into a Codex chat.

In a new Codex chat with the AGM plugin enabled, ask AGM to find the endpoint you need. For example: “Use AGM to find the available risk archetype definitions.” The connector tools are:

- `find_endpoints`: Search or page through the documented operations.
- `describe_endpoint`: Inspect an exact method and route, including the tool used to invoke it.
- `read_api`: Run a reviewed GET operation. Array query values repeat the key, as in `id=first&id=second`.
- `execute_api`: Run other documented operations after reviewing the method, path, query and JSON body. This includes some GET operations, such as ETL dispatch. Codex must have explicit authorization for the operation, and the local macOS dialog must receive **Allow once**. Cancel, timeout or failure to open the dialog sends no request.

Use the exact route template shown in the catalog and supply dynamic values in `path_parameters`. JSON bodies pass through to AGM unchanged in meaning. The documented uploads use JSON fields for their file data. For large JSON, CSV or binary results, ask for `save_response` before the API call; the result contains a path to the private local response file, with a 128 MiB download limit. Inline responses are capped at 256 KiB. Delete saved files when finished. A timeout after sending a request does not prove the operation failed; check AGM before trying again.

To refresh the installed catalog and connector, run `./integrations/codex/launch --help` for the local bridge command options. From the `agm-api` checkout, refresh the plugin itself with:

```sh
python3 -B integrations/codex/install_plugin.py
codex plugin add agm@agm
```

The first command preserves the plugin's existing skills and bundles the current local copy of the generated API docs. Routes in that copy may require an API release before they work on the live host. Open a new Codex chat to load the updated tools.
