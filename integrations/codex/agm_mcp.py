#!/usr/bin/env python3
"""AGM local MCP bridge. No dependencies beyond Python 3.10+."""
import argparse
import base64
import getpass
import json
import os
import re
import stat
import sys
import tempfile
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

ORIGIN = "https://api.agmtechnology.com"
VERSION = "0.3.8"
DOCS = Path(__file__).with_name("docs.html")
if not DOCS.is_file():  # Development checkout; installed plugins bundle a snapshot.
    DOCS = Path(__file__).resolve().parents[2] / "public/static/docs.html"
STATE = Path.home() / ".local/state/agm-codex"
AUTH = {"/token", "/users/login"}
PUBLIC_READS = {"/", "/docs", "/application_providers/read"}
# A GET can dispatch jobs or send email. Allow only reviewed read routes.
READS = PUBLIC_READS | {
    "/accounts/comments", "/accounts/contacts", "/accounts/instructions", "/accounts/read",
    "/accounts/ibkr/financial_ranges", "/accounts/ibkr/business_and_occupation",
    "/accounts/ibkr/product_country_bundles", "/accounts/ibkr/complex_asset_transfer_brokers",
    "/advisors/me/accounts", "/advisors/me/latest_nav", "/advisors/me/nav",
    "/advisors/me/account_contacts", "/advisors/me/account_proposals",
    "/advisors/me/open_positions", "/advisors/read",
    "/contacts/read", "/contacts/screening", "/contacts/documents",
    "/contacts/document_review", "/contacts/document_review/<uuid:document_id>",
    "/document_review_emails/read", "/document_review_messages/read", "/document_review_responsibles/read",
    "/fee_template_requests/read", "/flagged_deposits/read", "/flagged_deposits/reviews", "/advisor_changes/read",
    "/risk_profiles/list", "/risk_profiles/read", "/investment_proposals/read",
    "/reporting/tasks_review", "/reporting/pending_tasks", "/reporting/registration_tasks",
    "/reporting/compliance_tasks", "/trade_tickets/list", "/trade_tickets/read",
    "/reporting/rtd", "/reporting/ust_bonds", "/reporting/stocks", "/reporting/etfs",
    "/reporting/proposals_equity", "/reporting/clients", "/reporting/clients/fees",
    "/reporting/nav", "/reporting/nav/monthly", "/reporting/open_positions",
    "/reporting/trades", "/reporting/deposits_withdrawals", "/reporting/deposits_withdrawals/monthly",
    "/reporting/brokerage_commissions", "/reporting/management_commissions",
    "/reporting/ending_balances_from_statements", "/reporting/ibkr_details",
    "/reporting/ibkr_statement_summary", "/reporting/ibkr_statement_transactions",
    "/users/read",
}
MAX_INLINE = 256 * 1024
MAX_DOWNLOAD = 128 * 1024 * 1024
SUPPORTED_PROTOCOLS = ("2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05")
SEGMENT = r"[A-Za-z0-9_-]+"
VAR = re.compile(r"<(?:(uuid|string|int):)?([A-Za-z_]\w*)>")
UUID = r"[a-fA-F0-9]{8}(?:-[a-fA-F0-9]{4}){3}-[a-fA-F0-9]{12}"
TOKEN = re.compile(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")


LOGIN_COMMAND = '"$HOME/Library/Mobile Documents/com~apple~CloudDocs/Codex/Marketplaces/agm/plugins/agm/mcp/launch" login'
SERVER_INSTRUCTIONS = (
    "Find and describe documented AGM endpoints first; use read_api for data. "
    "Only reviewed read-only GET routes are available; changing records, sending email "
    "and triggering ETL are outside this connector. Never ask for an AGM password or JWT "
    f"in chat. If sign-in is required, ask the user to run {LOGIN_COMMAND} in Terminal "
    "and refresh the session before further API calls. "
    "Use GET /reporting/rtd for corporate bonds and GET /reporting/ust_bonds for U.S. Treasuries. "
    "For broad or historical reports, plan small, nonoverlapping batches using documented "
    "date filters or pagination, preserving all other filters. If no supported partition "
    "exists, use the documented report and disclose any size limit reached. "
    "Use save_response=true for large row sets and run at most two independent requests "
    "at a time. With cursor pagination, inspect each page before requesting the next. "
    "If a batch times out or exceeds the download limit, split it using supported filters "
    "and retain successful results. Read saved private response files locally; verify row "
    "counts, dates and coverage. Distinguish periods with rows, periods without rows and "
    "failed reads. No rows does not establish zero activity; rows can represent a partial "
    "snapshot. Verify freshness using a source timestamp or separate publication evidence; "
    "otherwise state that freshness could not be verified."
)
TRADE_GUIDANCE = (
    "Use explicit years and months: one year and at most two months per call. "
    "For a past year, cover all twelve months unless the user narrows the scope; for the "
    "current year, cover January through the current month in the user's timezone. "
    "months must be a single comma-separated string, e.g. "
    'query={"years":"2026","months":"01,02"} with save_response=true. '
    "Continue with 03,04, 05,06 and the rest of the requested period. Split a two-month "
    "batch into single months on timeout or download-limit errors. Check returned Year "
    "and Month against every requested month. Use TradeDate for the last trade date "
    'actually present. Count only rows where LevelOfDetail == "EXECUTION" when reporting '
    "executions; the report also includes order, lot and summary rows."
)


class BridgeError(Exception):
    pass


class RouteTable(HTMLParser):
    """Read the generated five-column API table and preserve parameter notes."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.routes = {}
        self.row = None
        self.cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.row = []
        elif tag == "td" and self.row is not None:
            self.cell = []
        elif tag in {"li", "br"} and self.cell is not None:
            self.cell.append("\n")

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag == "td" and self.cell is not None:
            self.row.append("".join(self.cell).strip())
            self.cell = None
        elif tag == "tr" and self.row:
            path, methods, description, query, _ = self.row
            for method in methods.split(", "):
                if method != "GET" or path not in READS:
                    continue
                if not path.startswith("/") or (path != "/" and not all(
                    re.fullmatch(SEGMENT, part) or VAR.fullmatch(part)
                    for part in path.removeprefix("/").split("/")
                )):
                    raise BridgeError("Unsupported path in AGM documentation.")
                if (method, path) in self.routes:
                    raise BridgeError("Duplicate operation in AGM documentation.")
                self.routes[method, path] = {
                    "method": method, "path": path,
                    "description": " ".join(description.split()),
                    "path_parameters": [name for _, name in VAR.findall(path)],
                    "query_parameters": query.splitlines(),
                    "tool": "read_api",
                }
            self.row = None


def catalog():
    table = RouteTable()
    table.feed(DOCS.read_text(encoding="utf-8"))
    table.close()
    if not table.routes:
        raise BridgeError("The bundled AGM documentation contains no routes.")
    trades = table.routes.get(("GET", "/reporting/trades"))
    if trades is not None:
        trades["usage_notes"] = TRADE_GUIDANCE
    return table.routes


def private_state():
    STATE.mkdir(parents=True, mode=0o700, exist_ok=True)
    meta = STATE.lstat()
    if not stat.S_ISDIR(meta.st_mode) or meta.st_uid != os.getuid() or meta.st_mode & 0o077:
        raise BridgeError("AGM state directory must be owned by you and accessible only to you.")


def load_token():
    try:
        private_state()
        fd = os.open(STATE / "access_token", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        raise BridgeError("Sign in locally with the AGM connector's login command.") from None
    with os.fdopen(fd, "r", encoding="ascii") as stream:
        meta = os.fstat(stream.fileno())
        if not stat.S_ISREG(meta.st_mode) or meta.st_uid != os.getuid() or meta.st_mode & 0o077:
            raise BridgeError("The AGM token file must be private and owned by you.")
        token = stream.read(8192).strip()
    if not isinstance(token, str) or not TOKEN.fullmatch(token):
        raise BridgeError("Invalid local AGM session. Sign in again.")
    return token


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


HTTP = build_opener(ProxyHandler({}), NoRedirect())


def send(method, path, query="", body=None, authorized=True):
    headers = {"Accept": "*/*"}
    if authorized:
        headers["Authorization"] = "Bearer " + load_token()
    if body is not None:
        headers["Content-Type"] = "application/json"
    request = Request(ORIGIN + path + ("?" + query if query else ""),
                      method=method, headers=headers, data=body)
    try:
        return HTTP.open(request, timeout=45)
    except HTTPError as error:
        return error
    except (URLError, TimeoutError):
        raise BridgeError("Could not complete the AGM API request. Check the AGM API connection.") from None


def login(path):
    if not sys.stdin.isatty():
        raise BridgeError("Run login in your own terminal. Never enter your password in chat.")
    private_state()
    email = input("AGM email: ").strip()
    password = getpass.getpass("AGM password: ")
    body = json.dumps({"email": email, "password": password}).encode()
    with send("POST", path, body=body, authorized=False) as response:
        if response.status >= 300:
            raise BridgeError(f"AGM sign-in failed (HTTP {response.status}).")
        data = json.loads(response.read(MAX_INLINE))
    token = data.get("access_token", "")
    if not isinstance(token, str) or not TOKEN.fullmatch(token):
        raise BridgeError("AGM did not return a valid session token.")
    fd, name = tempfile.mkstemp(prefix=".token-", dir=STATE)
    try:
        with os.fdopen(fd, "w", encoding="ascii") as stream:
            stream.write(token)
        os.replace(name, STATE / "access_token")
    finally:
        Path(name).unlink(missing_ok=True)
    print("Signed in to AGM. The connector can now use your account's API permissions.")


def render_path(template, parameters):
    if not isinstance(parameters, dict) or parameters.keys() != set(
        name for _, name in VAR.findall(template)
    ):
        raise BridgeError("Supply exactly the documented path parameters.")

    def replace(match):
        kind, key = match.groups()
        value = parameters[key]
        pattern = {"uuid": UUID, "int": r"[0-9]+"}.get(kind, SEGMENT)
        if not isinstance(value, (str, int)) or not re.fullmatch(pattern, str(value)):
            raise BridgeError(f"Invalid path parameter: {key}.")
        return str(value)

    return VAR.sub(replace, template)


def query_string(query):
    if not isinstance(query, dict):
        raise BridgeError("query must be an object.")
    pairs = []
    for key, values in query.items():
        for value in values if isinstance(values, list) else [values]:
            if type(value) not in {str, int, bool, float}:
                raise BridgeError("Query values must be strings, numbers, booleans, or arrays of those.")
            pairs.append((key, str(value).lower() if isinstance(value, bool) else value))
    return urlencode(pairs)


def save_result(response):
    private_state()
    fd, fname = tempfile.mkstemp(prefix="response-", dir=STATE)
    try:
        with os.fdopen(fd, "wb") as stream:
            size = 0
            while chunk := response.read(65536):
                size += len(chunk)
                if size > MAX_DOWNLOAD:
                    raise BridgeError("Response exceeded the 128 MiB local download limit.")
                stream.write(chunk)
    except BaseException:
        Path(fname).unlink(missing_ok=True)
        raise
    return {"saved_to": fname, "size_bytes": size}


def response_result(response, save_response):
    info = {"status": response.status, "content_type": response.headers.get_content_type()}
    if disposition := response.headers.get("Content-Disposition"):
        info["content_disposition"] = disposition
    if 300 <= response.status < 400:
        return {**info, "message": "Upstream redirect blocked; no credentials were forwarded."}
    if response.status == 401:
        info["session_help"] = f"Sign in again from Terminal: {LOGIN_COMMAND}"
    if save_response and response.status < 300:
        return {**info, **save_result(response)}
    data = response.read(MAX_INLINE + 1)
    if len(data) > MAX_INLINE:
        return {**info, "message": "Response exceeds the inline limit. Use filters or request save_response on your next authorized call."}
    mime = info["content_type"]
    if mime == "application/json" or mime.endswith("+json"):
        try:
            info["body"] = json.loads(data) if data else None
            return info
        except (ValueError, UnicodeError):
            pass
    if mime.startswith("text/") or mime == "application/xml":
        info["body"] = data.decode("utf-8", errors="replace")
    else:
        info["body_base64"] = base64.b64encode(data).decode("ascii")
    return info


OP_SCHEMA = {
    "method": {"type": "string", "description": "HTTP method, e.g. GET."},
    "path": {"type": "string", "description": "Exact documented path template, including any <parameters>."},
}
REQUEST_SCHEMA = {
    **OP_SCHEMA,
    "path_parameters": {"type": "object", "description": "Values for placeholders in the documented path."},
    "query": {"type": "object", "description": "Query parameters; arrays repeat a key."},
    "save_response": {"type": "boolean", "description": "Save the raw successful response to a private local file instead of returning its contents."},
}


def tool(name, description, properties, required=()):
    return {"name": name, "description": description,
            "inputSchema": {"type": "object", "properties": properties,
                            "required": list(required), "additionalProperties": False},
            "annotations": {"readOnlyHint": True, "destructiveHint": False,
                            "idempotentHint": True, "openWorldHint": True}}


TOOLS = [
    tool("find_endpoints", "Search the read-only AGM API catalog and get exact parameter notes. Omit search to browse; use offset to paginate.",
         {"search": {"type": "string"}, "offset": {"type": "integer", "minimum": 0},
          "limit": {"type": "integer", "minimum": 1, "maximum": 30}}),
    tool("describe_endpoint", "Describe one exact AGM API operation and identify the tool needed to invoke it.",
         OP_SCHEMA, ("method", "path")),
    tool("read_api", "Read data from an AGM API endpoint listed as read_api. Uses the local AGM user's bearer session. Batch large reports, verify period coverage, and use save_response for large results.",
         REQUEST_SCHEMA, ("method", "path")),
]


def call_tool(name, args, routes):
    if name == "find_endpoints":
        words = args.get("search", "").casefold().split()
        offset, limit = args.get("offset", 0), args.get("limit", 15)
        if type(offset) is not int or type(limit) is not int or offset < 0 or not 1 <= limit <= 30:
            raise BridgeError("Invalid catalog pagination.")
        matches = [op for op in routes.values()
                   if all(word in json.dumps(op).casefold() for word in words)]
        end = offset + limit
        return {"total": len(matches), "next_offset": end if end < len(matches) else None,
                "operations": matches[offset:end]}
    operation = routes.get((args["method"].upper(), args["path"]))
    if operation is None:
        raise BridgeError("This endpoint is unavailable in the read-only AGM connector. Use find_endpoints.")
    if name == "describe_endpoint":
        return operation
    path = render_path(operation["path"], args.get("path_parameters", {}))
    query = query_string(args.get("query", {}))
    with send("GET", path, query, authorized=path not in PUBLIC_READS) as response:
        return response_result(response, args.get("save_response") is True)


def dispatch(message, routes):
    method = message["method"]
    if method == "initialize":
        requested = message.get("params", {}).get("protocolVersion")
        return {"protocolVersion": requested if requested in SUPPORTED_PROTOCOLS else SUPPORTED_PROTOCOLS[0],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "AGM-API", "version": VERSION},
                "instructions": SERVER_INSTRUCTIONS}
    if method == "ping":
        return {}
    if method == "tools/list":
        return {"tools": TOOLS}
    if method == "tools/call":
        params = message["params"]
        name = params.get("name")
        args = params.get("arguments", {})
        schema = next((entry["inputSchema"] for entry in TOOLS if entry["name"] == name), None)
        try:
            if not schema or not isinstance(args, dict) or not (
                set(schema["required"]) <= args.keys() <= schema["properties"].keys()
            ):
                raise BridgeError("Unknown tool or invalid arguments.")
            result = call_tool(name, args, routes)
            return {"isError": result.get("status", 200) >= 300,
                    "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}]}
        except BridgeError as error:
            return {"isError": True, "content": [{"type": "text", "text": str(error)}]}
        except (ValueError, TypeError, AttributeError, KeyError):
            return {"isError": True, "content": [{"type": "text", "text": "Invalid AGM tool arguments or response format."}]}
        except (OSError, TimeoutError):
            return {"isError": True, "content": [{"type": "text", "text": "Local storage or upstream transport failed. Check the AGM API connection."}]}
    raise KeyError(method)


def serve():
    routes = catalog()
    for line in sys.stdin:
        reply = {"jsonrpc": "2.0", "id": None}
        try:
            message = json.loads(line)
        except ValueError:
            reply["error"] = {"code": -32700, "message": "Parse error"}
        else:
            if not isinstance(message, dict) or message.get("jsonrpc") != "2.0" or not isinstance(message.get("method"), str):
                reply["error"] = {"code": -32600, "message": "Invalid request"}
            elif "id" not in message:
                continue
            else:
                reply["id"] = message["id"]
                try:
                    reply["result"] = dispatch(message, routes)
                except KeyError:
                    reply["error"] = {"code": -32601, "message": "Method not found"}
                except (ValueError, TypeError, AttributeError):
                    reply["error"] = {"code": -32602, "message": "Invalid params"}
        print(json.dumps(reply, ensure_ascii=False), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", nargs="?", choices=["serve", "login", "logout"], default="serve")
    parser.add_argument("--endpoint", choices=sorted(AUTH), default="/token")
    args = parser.parse_args()
    try:
        if args.command == "login":
            login(args.endpoint)
        elif args.command == "logout":
            private_state()
            (STATE / "access_token").unlink(missing_ok=True)
            print("Local AGM session removed.")
        else:
            serve()
    except (BridgeError, OSError, ValueError) as error:
        print(str(error) if isinstance(error, BridgeError) else "AGM connector startup or sign-in failed.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
