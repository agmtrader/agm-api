#!/usr/bin/env python3
"""Read one IBKR account and generate a local PortfolioAnalyst-style PDF.

Run from agm-api: PYTHONDONTWRITEBYTECODE=1 venv/bin/python -B dev/portfolio_analyst.py
Authentication uses the existing Secret Manager signing key and ADC. No Flask
startup, schema initialization, trading session competition, orders or retries.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from io import BytesIO
import ipaddress
import json
import math
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_ACCOUNT = "U20566763"
DEFAULT_MASTER = "F10740574"
DEFAULT_CREDENTIAL = "aguiagm2024"
DEFAULT_REFERENCE = Path.home() / "Downloads/AGM_Technology_(BVI)_Inc._Inception_September_28_2026 (1).pdf"


class ReportError(Exception):
    """A bounded diagnostic safe to display without provider payloads/tokens."""


class PAClient:
    """Target-master entitlement plus explicitly configured SSO provisioning."""

    def __init__(self, master=DEFAULT_MASTER, credential=DEFAULT_CREDENTIAL, sso_master="I6413690"):
        from src.utils.connectors.ibkr_web_api import IBKRWebAPI
        if master not in IBKRWebAPI.MASTER_ACCOUNT_CREDENTIALS:
            raise ReportError("Unsupported master account")
        self.master = master
        self.sso_master = sso_master
        self.credential = credential
        self.signer = None
        self.sso_token = None
        self.receipts = []

    def request(self, method, path, payload=None, token=None, signed=False, params=None):
        import requests
        allowed = {"/oauth2/api/v1/token", "/gw/api/v1/sso-sessions",
                   "/v1/api/portfolio/accounts", "/v1/api/pa/performance",
                   "/v1/api/pa/allperiods", "/v1/api/pa/allocation", "/v1/api/trsrv/secdef"}
        if path not in allowed and not re.fullmatch(r"/gw/api/v1/accounts/U\d+/details", path) and not re.fullmatch(
            r"/v1/api/portfolio/U\d+/(meta|summary|ledger|positions/\d+)", path
        ):
            raise ReportError("Request is outside the report read allowlist")
        headers = {"Content-Type": "application/json"}
        if token or self.sso_token:
            headers["Authorization"] = "Bearer " + (token or self.sso_token)
        kwargs = {"headers": headers, "timeout": (10, 60), "allow_redirects": False}
        if params is not None:
            kwargs["params"] = params
        if signed:
            headers["Content-Type"] = "application/jwt"
            kwargs["data"] = self.signer.sign_request(payload)
        elif path == "/oauth2/api/v1/token":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            kwargs["data"] = payload
        elif payload is not None:
            kwargs["json"] = payload
        try:
            response = requests.request(method, "https://api.ibkr.com" + path, **kwargs)
        except requests.RequestException as exc:
            raise ReportError(f"{method} {path}: {type(exc).__name__}; not retried") from None
        self.receipts.append({"method": method, "path": path, "status": response.status_code,
                              "at": datetime.now(timezone.utc).isoformat()})
        if response.status_code != 200:
            raise ReportError(f"{method} {path}: HTTP {response.status_code}; not retried")
        try:
            result = response.json()
        except ValueError:
            raise ReportError(f"{method} {path}: expected JSON") from None
        if isinstance(result, dict) and (result.get("error") or result.get("rc") not in (None, 0)):
            raise ReportError(f"{method} {path}: provider reported an error")
        return result

    def oauth(self, master):
        self.signer._apply_credentials(master)
        oauth = self.request("POST", "/oauth2/api/v1/token", {
            "grant_type": "client_credentials",
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": self.signer.generate_client_assertion(),
            "scope": "accounts.read sso-sessions.write",
        })
        token = oauth.get("access_token")
        if not isinstance(token, str) or not token:
            raise ReportError("OAuth response did not contain an access token")
        return token

    def connect(self, ip=None):
        import requests
        from src.utils.connectors.ibkr_web_api import IBKRWebAPI
        self.signer = IBKRWebAPI()
        if self.sso_master not in self.signer.MASTER_ACCOUNT_CREDENTIALS:
            raise ReportError("Unsupported SSO provisioning context")
        # Management entitlement and SSO provisioning are distinct. Actual
        # October 27 reads verified this FA account using FA2; the username's
        # SSO is provisioned under FD2. FA2 SSO returned 403. No blind fallback.
        self.management_token = self.oauth(self.master)
        token = self.oauth(self.sso_master) if self.sso_master != self.master else self.management_token
        if ip is None:
            try:
                response = requests.get("https://api.ipify.org", timeout=(10, 10))
                response.raise_for_status()
                ip = response.text.strip()
            except requests.RequestException:
                raise ReportError("Unable to determine public IP; supply --ip") from None
        ipaddress.ip_address(ip)
        sso = self.request("POST", "/gw/api/v1/sso-sessions",
                           {"credential": self.credential, "ip": ip}, token=token, signed=True)
        self.sso_token = sso.get("access_token")
        if not isinstance(self.sso_token, str) or not self.sso_token:
            raise ReportError("SSO response did not contain an access token")
        # PA/portfolio reads use the outer session. Do not compete for an
        # iserver brokerage session or log out a user's existing broker session.

    def collect(self, account=DEFAULT_ACCOUNT, period="1Y"):
        if not re.fullmatch(r"U\d+", account):
            raise ReportError("Expected a U-account identifier")
        bundle = {"account": account, "master": self.master,
                  "sso_master": self.sso_master,
                  "requested_period": period, "collected_at": datetime.now(timezone.utc).isoformat(),
                  "errors": {}, "receipts": self.receipts}
        def optional(name, method, path, payload=None):
            try:
                bundle[name] = self.request(method, path, payload)
            except ReportError as exc:
                bundle["errors"][name] = str(exc)
        details = self.request("GET", f"/gw/api/v1/accounts/{account}/details",
                               token=self.management_token).get("account", {})
        if details.get("accountId") != account or details.get("masterAccountId") != self.master:
            raise ReportError("Account identity/master does not match the requested report")
        bundle["account_info"] = {k: details.get(k) for k in
                                  ("accountId", "masterAccountId", "baseCurrency", "accountTitle",
                                   "applicantType", "dateOpened")}
        # One request per PA endpoint, no automatic refresh/replay. Raw payloads
        # stay in memory; the requested PDF is the only generated output.
        bundle["performance"] = self.request("POST", "/v1/api/pa/performance",
                                             {"acctIds": [account], "period": period})
        included = bundle["performance"].get("included")
        if included is not None and included != [account]:
            raise ReportError("Performance response does not include exactly the requested account")
        optional("allperiods", "POST", "/v1/api/pa/allperiods", {"acctIds": [account]})
        if "allperiods" in bundle:
            raw = bundle["allperiods"]
            if raw.get("pm") != bundle["performance"].get("pm"):
                bundle["errors"]["allperiods"] = "Performance measures disagree; comparisons omitted"
                del bundle["allperiods"]
            else:
                bundle["allperiods"] = {account: raw.get(account, {}), "pm": raw.get("pm")}
        optional("allocation", "POST", "/v1/api/pa/allocation",
                 {"acctIds": [account], "type": "ALL", "currency": details["baseCurrency"]})
        if "allocation" in bundle and account in bundle["allocation"].get("excluded", []):
            bundle["errors"]["allocation"] = "Requested account excluded by provider"
            del bundle["allocation"]
        optional("accounts", "GET", "/v1/api/portfolio/accounts")
        # Portfolio metadata and holdings are current, not historical PA data.
        optional("meta", "GET", f"/v1/api/portfolio/{account}/meta")
        if "meta" in bundle and (bundle["meta"].get("accountId") != account
            or bundle["meta"].get("master", {}).get("id") != self.master):
            raise ReportError("Trading portfolio metadata disagrees with account/master identity")
        optional("summary", "GET", f"/v1/api/portfolio/{account}/summary")
        optional("ledger", "GET", f"/v1/api/portfolio/{account}/ledger")
        positions = []
        position_keys = set()
        for page in range(1000):
            try:
                rows = self.request("GET", f"/v1/api/portfolio/{account}/positions/{page}")
                if not isinstance(rows, list):
                    raise ReportError("Positions response was not an array")
                if any(r.get("acctId", account) != account for r in rows):
                    raise ReportError("Positions response contains a different account")
                for row in rows:
                    key = (row.get("conid"), row.get("model"))
                    if key in position_keys:
                        raise ReportError("Repeated position across pages; incomplete data omitted")
                    position_keys.add(key)
                positions.extend(rows)
                if len(rows) < 100:
                    bundle["positions"] = positions
                    break
            except ReportError as exc:
                bundle["errors"]["positions"] = str(exc)
                break
        else:
            bundle["errors"]["positions"] = "Position pagination safety bound reached; incomplete data omitted"
        definitions = []
        if "positions" in bundle:
            conids = sorted({int(p["conid"]) for p in positions})
            try:
                for offset in range(0,len(conids),200):
                    raw = self.request("GET", "/v1/api/trsrv/secdef",
                                       params={"conids": ",".join(map(str,conids[offset:offset+200]))})
                    definitions.extend(raw.get("secdef", []))
                bundle["contracts"] = definitions
            except ReportError as exc:
                bundle["errors"]["contracts"] = str(exc)
        bundle.pop("accounts", None)
        return bundle


def finite(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (ValueError, TypeError):
        return None
    return number if math.isfinite(number) else None


def account_series(section, account, value_key):
    if not isinstance(section, dict):
        return [], []
    matches = [row for row in section.get("data", []) if row.get("id") == account]
    if len(matches) != 1:
        raise ReportError(f"Missing or ambiguous {value_key} series for requested account")
    dates, values = section.get("dates", []), matches[0].get(value_key, [])
    if not dates or len(dates) != len(values):
        raise ReportError(f"Mismatched {value_key} dates and values")
    if any(finite(v) is None for v in values):
        raise ReportError(f"Invalid {value_key} values")
    if dates != sorted(set(dates)):
        raise ReportError(f"Unordered or duplicated {value_key} dates")
    return dates, [float(v) for v in values]


def shape(value, depth=0):
    """Describe provider schemas without logging balances, people or tokens."""
    if depth > 3:
        return type(value).__name__
    if isinstance(value, dict):
        return {k: shape(v, depth + 1) for k, v in value.items()
                if k not in ("access_token", "Authorization", "credential")}
    if isinstance(value, list):
        return {"length": len(value), "first_shape": shape(value[0], depth + 1) if value else None}
    return type(value).__name__


def bond_data(bundle):
    """Resolve face units and scheduled flows using actual contract definitions.

    CF table payments are expressed on the table's redemption notional. Only
    a single fixed, bullet redemption schedule is normalized here. Amortizing,
    floating, defaulted or ambiguous contracts remain unsupported.
    """
    from datetime import timedelta
    definitions = {int(d["conid"]):d for d in bundle.get("contracts", [])}
    asof = datetime.fromisoformat(bundle["collected_at"]).date()
    result = []
    for position in bundle.get("positions", []):
        if position.get("assetClass") != "BOND":
            continue
        definition = definitions.get(int(position["conid"]), {})
        bond = definition.get("bond", {})
        coupons = bond.get("coupon", [])
        face_unit = finite(bond.get("principalValue"))
        quantity = finite(position.get("position"))
        face = face_unit*quantity if face_unit is not None and quantity is not None else None
        row = {"position":position,"definition":definition,"face":face,
               "name":definition.get("bondName") or definition.get("fullName") or position.get("contractDesc"),
               "currency":position.get("currency"),"maturity":bond.get("maturityDate"),"flows":[],
               "coupon_rate":None,"annual_income":None,"schedule_supported":False}
        if (len(coupons)==1 and bond.get("couponType")=="FIXED"
            and bond.get("payPrincipalOnMaturity")=="T" and bond.get("isDefaulted")!="Y"
            and face is not None and bond.get("principalValueCurr")==position.get("currency")):
            coupon = coupons[0]
            flows = coupon.get("cfTable", {}).get("cf", [])
            principals = [f for f in flows if finite(f.get("pp")) not in (None,0)]
            if len(principals)==1 and str(principals[0].get("cd"))==bond.get("maturityDate"):
                notional = finite(principals[0].get("pp"))
                rate = finite(coupon.get("rate"))
                if notional is not None and notional>0 and rate is not None:
                    for flow in flows:
                        date = datetime.strptime(str(flow["cd"]), "%Y%m%d").date()
                        interest, principal = finite(flow.get("ip")), finite(flow.get("pp"))
                        if interest is None or principal is None:
                            break
                        if asof < date <= asof+timedelta(days=3*366):
                            row["flows"].append({"date":date,"interest":interest*face/notional,
                                                 "principal":principal*face/notional})
                    else:
                        row.update(coupon_rate=rate, annual_income=face*rate/100, schedule_supported=True)
        result.append(row)
    return result


def reference_font(document, entry):
    """Restore PDF-stripped TTF metadata in memory, retaining original glyphs.

    This reference embeds an Identity-H subset with no name/post/cmap tables.
    Recover the Unicode mapping from its ToUnicode CMap, not a guessed font.
    """
    import struct
    from reportlab.pdfbase.ttfonts import TTFontMaker
    raw = document.extract_font(entry[0])[3]
    tables = {}
    for n in range(struct.unpack_from(">H", raw, 4)[0]):
        tag, _, offset, length = struct.unpack_from(">4sIII", raw, 12+n*16)
        tables[tag.decode()] = raw[offset:offset+length]
    if "cmap" in tables and "name" in tables and "post" in tables:
        return raw
    descriptor = document.xref_object(entry[0])
    desc_ref = re.search(r"/DescendantFonts\s*\[\s*(\d+)", descriptor)
    cmap_ref = re.search(r"/ToUnicode\s+(\d+)", descriptor)
    if not desc_ref or not cmap_ref or "/CIDToGIDMap /Identity" not in document.xref_object(int(desc_ref[1])):
        raise ReportError("Unsupported reference font encoding")
    cmap_text = document.xref_stream(int(cmap_ref[1])).decode("ascii")
    mapping = {}
    for block in re.findall(r"beginbfrange(.*?)endbfrange", cmap_text, re.S):
        for first, last, unicode_first in re.findall(r"<([0-9a-fA-F]+)>\s*<([0-9a-fA-F]+)>\s*<([0-9a-fA-F]+)>",block):
            for gid in range(int(first,16),int(last,16)+1):
                mapping[int(unicode_first,16)+gid-int(first,16)] = gid
    if not mapping:
        raise ReportError("Reference font Unicode mapping is missing")
    # Format 4 Unicode cmap: one constant-delta segment per character.
    codes = sorted(mapping)+[65535]
    count = len(codes)
    power = 2**int(math.log2(count))
    pack = lambda values: struct.pack(">"+"H"*len(values), *values)
    sub = struct.pack(">7H",4,16+8*count,0,2*count,2*power,int(math.log2(power)),2*count-2*power)
    sub += pack(codes)+pack([0])+pack(codes)
    sub += pack([(mapping.get(k,0)-k)&65535 for k in codes])+pack([0]*count)
    tables["cmap"] = struct.pack(">HHHHI",0,1,3,1,12)+sub
    names = {1:"DroidSans",2:"Regular",3:"PAReferenceDroidSans",4:"DroidSans",6:"DroidSans"}
    strings = b""
    records = b""
    for key, value in names.items():
        encoded = value.encode("utf-16-be")
        records += struct.pack(">6H",3,1,1033,key,len(encoded),len(strings))
        strings += encoded
    tables["name"] = struct.pack(">3H",0,len(names),6+12*len(names))+records+strings
    tables["post"] = struct.pack(">IihhIIIII",0x00030000,0,-100,50,0,0,0,0,0)
    maker = TTFontMaker()
    for tag, data in tables.items():
        maker.add(tag,data)
    return maker.makeStream()


def render_report(bundle, reference=None):
    """Render real response data; unavailable PDF-only analytics stay unavailable.

    All charts/tables are vector PDF. The optional reference contributes only
    its font and cover artwork; never any account data, charts or balances.
    """
    from calendar import month_name
    from collections import defaultdict
    from decimal import Decimal, ROUND_HALF_EVEN
    import fitz
    from reportlab.pdfgen import canvas
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib.colors import HexColor
    from reportlab.platypus import Paragraph
    from reportlab.lib.styles import ParagraphStyle
    from xml.sax.saxutils import escape
    from html import unescape

    account = bundle["account"]
    info = bundle.get("account_info", {})
    if info.get("accountId") != account or info.get("masterAccountId") != bundle["master"]:
        raise ReportError("Verified account/master metadata is required before rendering")
    perf = bundle["performance"]
    dates, navs = account_series(perf.get("nav"), account, "navs")
    return_dates, returns = account_series(perf.get("cps"), account, "returns")
    if dates != return_dates:
        raise ReportError("NAV and return date coverage disagree")
    measure = perf.get("pm")
    if measure not in ("TWR", "MWR"):
        raise ReportError("Provider performance measure is unavailable")
    base = info.get("baseCurrency")
    nav_row = next(r for r in perf["nav"]["data"] if r.get("id") == account)
    if nav_row.get("baseCurrency") != base:
        raise ReportError("NAV and account base currencies disagree")
    for name in ("cps","tpps"):
        rows = perf.get(name,{}).get("data",[])
        if any(r.get("id")==account and r.get("baseCurrency")!=base for r in rows):
            raise ReportError("Performance series base currencies disagree")
    reference_doc = fitz.open(reference) if reference and Path(reference).is_file() else None
    font = "Helvetica"
    if reference_doc:
        for entry in reference_doc[1].get_fonts():
            if "DroidSans" in entry[3]:
                _, ext, _, content = reference_doc.extract_font(entry[0])
                if ext == "ttf":
                    pdfmetrics.registerFont(TTFont("PAReference", BytesIO(reference_font(reference_doc, entry))))
                    font = "PAReference"
                    break
    stream = BytesIO()
    c = canvas.Canvas(stream, pagesize=(792, 612), pageCompression=1)
    c.setTitle(f"{account} - Portfolio Analysis - {bundle['requested_period']}")
    c.setAuthor("AGM Technology")
    ink, muted, line, blue = map(HexColor, ("#222222", "#7C8FA6", "#E2E2E2", "#0078C5"))
    palette = ["#0078B9", "#1BA9E8", "#FDBB18", "#F6A800", "#85BB25", "#F65B16"]

    def text(value, x, y, size=6.5, color=None, right=False):
        c.setFillColor(color or ink)
        glyphs = getattr(pdfmetrics.getFont(font).face,"charToGlyph",None)
        active_font = "Helvetica" if glyphs is not None and any(ord(ch) not in glyphs for ch in str(value)) else font
        c.setFont(active_font, size)
        (c.drawRightString if right else c.drawString)(x, 612-y, str(value))

    def rule(x1, y, x2):
        c.setStrokeColor(line)
        c.setLineWidth(.5)
        c.line(x1, 612-y, x2, 612-y)

    def money(value):
        if finite(value) is None:
            return "-"
        return f"{Decimal(str(value)).quantize(Decimal('.01'), rounding=ROUND_HALF_EVEN):,.2f}"

    def percent(value):
        return "-" if finite(value) is None else f"{float(value)*100:.2f}%"

    def date_label(value):
        try:
            return datetime.strptime(value[:8], "%Y%m%d").strftime("%B %d, %Y")
        except (ValueError, TypeError):
            return str(value)

    period_label = f"Analysis Period: {date_label(dates[0])} - {date_label(dates[-1])} ({measure})"
    current_label = "Current holdings retrieved: " + bundle["collected_at"][:10]

    def paragraph(value, x, y, width, size=7, color=ink):
        style = ParagraphStyle("PA", fontName=font, fontSize=size, leading=size*1.55,
                               textColor=color)
        glyphs = getattr(pdfmetrics.getFont(font).face,"charToGlyph",None)
        markup = "".join(escape(ch) if glyphs is None or ord(ch) in glyphs
                         else '<font name="Helvetica">'+escape(ch)+'</font>' for ch in str(value))
        p = Paragraph(markup, style)
        _, height = p.wrap(width, 500)
        p.drawOn(c, x, 612-y-height)
        return y + height

    def page(title, subtitle=""):
        text(title, 24, 34, 11, muted)
        text(subtitle, 768, 34, 6.3, muted, right=True)
        rule(24, 40, 768)
        rule(24, 558, 768)
        # Keep provenance in the footer; the report is an AGM reconstruction.
        text("AGM | IBKR API data", 24, 588, 5.5, muted)
        text(f"Page: {c.getPageNumber()}", 768, 588, 7, right=True)

    def table(headers, rows, x, y, widths, row_height=13, bars=False, left_columns=(0,)):
        for i, label in enumerate(headers):
            xx = x + sum(widths[:i])
            text(label, xx + (2 if i in left_columns else widths[i]-3), y, 6, right=i not in left_columns)
        rule(x, y+5, x+sum(widths))
        y += 18
        for row in rows:
            if y > 540:
                c.showPage()
                page("Open Position Summary (Cont.)", current_label)
                y = table(headers, [], x, 66, widths, row_height,left_columns=left_columns)
            for i, val in enumerate(row):
                xx = x+sum(widths[:i])
                limit = widths[i]-6
                val = str(val)
                while pdfmetrics.stringWidth(val, font, 6.2) > limit and len(val) > 2:
                    val = val[:-4] + "..."
                text(val, xx+(2 if i in left_columns else widths[i]-3), y, 6.2, right=i not in left_columns)
            rule(x, y+4, x+sum(widths))
            if bars:
                weight = finite(str(row[-1]).replace("%", ""))
                if weight is not None:
                    c.setFillColor(HexColor("#E5E5E5"))
                    c.rect(x+sum(widths)+4, 612-y-2, 56, 6, fill=1, stroke=0)
                    c.setFillColor(HexColor("#713B98"))
                    c.rect(x+sum(widths)+4, 612-y-2, 56*min(abs(weight)/100, 1), 6, fill=1, stroke=0)
            y += row_height
        return y

    def chart(title, ds, values, x, y, width, height, pct=False, bars=False):
        text(title, x, y, 11, muted)
        if not values:
            paragraph("Unavailable from the returned API data.", x, y+28, width)
            return
        left, top, w, h = x+40, y+20, width-48, height-58
        lo, hi = min(values), max(values)
        span = hi-lo or max(abs(hi)*.05, 1)
        lo -= span*.08
        hi += span*.08
        for tick in range(6):
            v = lo+(hi-lo)*tick/5
            yy = top+h-h*tick/5
            rule(left, yy, left+w)
            text(percent(v) if pct else f"{v:,.0f}", left-5, yy+2, 5.5, right=True)
        c.saveState()
        clip = c.beginPath()
        clip.rect(left, 612-top-h, w, h)
        c.clipPath(clip, stroke=0)
        c.setStrokeColor(blue)
        c.setFillColor(HexColor("#41A0CE"))
        c.setLineWidth(.8)
        path = c.beginPath()
        for n, v in enumerate(values):
            xx = left+n*w/max(len(values)-1, 1)
            yy = 612-top-h+(v-lo)*h/(hi-lo)
            if bars:
                bw = max(w/len(values)*.85, .5)
                c.rect(xx-bw/2, 612-top-h, bw, yy-(612-top-h), fill=1, stroke=0)
            elif n:
                path.lineTo(xx, yy)
            else:
                path.moveTo(xx, yy)
        if not bars:
            c.drawPath(path)
        c.restoreState()
        for n in sorted({round(k*(len(ds)-1)/5) for k in range(6)}):
            raw = ds[n]
            label = raw[4:6]+"/"+raw[6:8]+"/"+raw[2:4] if len(raw)==8 else raw
            text(label, left+n*w/max(len(ds)-1, 1)-13, top+h+12, 5)
        text(account + (" Return" if pct else " NAV"), left+w*.40, top+h+29, 5.5, blue)

    def allocation_rows(category):
        allocation = bundle.get("allocation", {})
        if allocation.get("currency") != base:
            return []
        result = []
        for side, side_data in allocation.get("allocations", {}).get(category, {}).items():
            for item in side_data.get("items", []):
                result.append([item.get("name", "-"), money(item.get("nav")),
                               percent(item.get("weight")), side])
        return result

    def missing(title, message, y=90):
        text(title, 30, y, 11, muted)
        paragraph(message, 30, y+20, 710, 8)

    # Cover is replaced by the reference's original artwork after rendering.
    text("PortfolioAnalyst", 50, 110, 28)
    c.setFillColor(HexColor("#4D6F9C"))
    c.rect(0, 380, 792, 84, fill=1, stroke=0)
    logo = ROOT.parent / "agm-dashboard/public/assets/brand/agm-logo.png"
    if logo.is_file():
        c.drawImage(str(logo), 330, 68, width=132, height=70, preserveAspectRatio=True, mask="auto")
    c.showPage()
    # TOC is populated from actual rendered section pages, including overflow.
    page("Table of Contents")
    c.showPage()
    contents = []
    def section(title, subtitle):
        contents.append((title, c.getPageNumber()))
        page(title, subtitle)

    section("Account Overview", period_label)
    text("NAME:", 32, 56, 5.5)
    text(info.get("accountTitle") or bundle.get("meta", {}).get("accountTitle") or "-", 130, 56, 5.5)
    text("ACCOUNT:", 32, 68, 5.5)
    text(account, 130, 68, 5.5)
    text("ACCOUNT TYPE:", 32, 80, 5.5)
    text(info.get("applicantType") or bundle.get("meta", {}).get("type", "-"), 130, 80, 5.5)
    text("PERFORMANCE MEASURE", 748, 56, 5.5, right=True)
    text(measure, 748, 68, 6, right=True)
    text("BASE CURRENCY  " + str(base), 748, 80, 5.5, right=True)
    rule(24, 94, 768)
    chart("Cumulative Return", return_dates, returns, 30, 113, 345, 214, pct=True)
    chart("Net Asset Value", dates, navs, 30, 345, 345, 197, bars=True)
    text("Key Statistics", 405, 113, 11, muted)
    periods = bundle.get("allperiods", {}).get(account, {})
    for n, key in enumerate([bundle["requested_period"], "7D", "MTD"]):
        vals = periods.get(key, {}).get("cps", [])
        value = returns[-1] if key == bundle["requested_period"] else vals[-1] if vals else None
        text(percent(value), 414+n*112, 143, 14,
             HexColor("#00A84E") if finite(value) is not None and value>=0 else HexColor("#E6222B"))
        text(key + " RETURN", 414+n*112, 159, 5.8)
    start_nav = nav_row.get("startNAV", {})
    summary_rows = [["Beginning NAV", money(start_nav.get("val"))],
                    ["Beginning NAV date", date_label(start_nav.get("date", ""))],
                    ["Ending NAV", money(navs[-1])],
                    ["Change in NAV", money(navs[-1]-float(start_nav["val"])) if finite(start_nav.get("val")) is not None else "-"],
                    ["Currency", base]]
    table(["Period values", ""], summary_rows, 407, 191, [205, 135], 14)
    paragraph("NAV change includes cash flows and is not investment profit. The API does not supply the reference report's full NAV-change bridge or per-security period-return rankings.", 409, 294, 331, 6.6, muted)
    text("Allocation", 405, 353, 11, muted)
    y = table(["Financial Inst.", "Value ("+base+")", "%", "Side"],
              allocation_rows("FINANCIAL_INSTRUMENT"), 407, 374, [140, 91, 53, 55], 13)
    table(["Asset Class", "Value ("+base+")", "%", "Side"],
          allocation_rows("ASSET_CLASS"), 407, max(y+20, 469), [140, 91, 53, 55], 13)
    c.showPage()

    section("Historical Performance", period_label)
    history_keys = [k for k in ("1D", "7D", "MTD", "1M", "YTD", "1Y") if periods.get(k, {}).get("cps")]
    text("History", 30, 69, 11, muted)
    rule(30, 270, 760)
    for n, key in enumerate(history_keys):
        val = finite(periods[key]["cps"][-1])
        xx = 74+n*115
        text(percent(val), xx, 137, 17, blue)
        text(key, xx, 160, 9, muted)
        ds = periods[key].get("dates", [])
        if ds:
            text(ds[0]+" - "+ds[-1], xx, 178, 5.5, muted)
    chart(bundle["requested_period"] + " Return", return_dates, returns, 30, 300, 730, 242, pct=True)
    c.showPage()

    section("Historical Performance (Cont.)", period_label)
    monthly_dates, monthly_values = account_series(perf.get("tpps"), account, "returns")
    if perf.get("tpps", {}).get("freq") != "M":
        raise ReportError("Monthly performance table requires provider monthly frequency")
    monthly = dict(zip(monthly_dates, monthly_values))
    years = sorted({d[:4] for d in monthly_dates}, reverse=True)
    rows = [[month_name[m]]+[percent(monthly.get(year+f"{m:02d}")) for year in years] for m in range(1,13)]
    table([""]+years, rows, 32, 83, [180]+[125]*len(years), 27)
    paragraph("The first and last displayed months may be partial. Values are the provider's time-period returns for this request; no quarter or full-year return is inferred from partial coverage.", 32, 465, 710, 7, muted)
    c.showPage()

    positions = bundle.get("positions", [])
    contracts = {int(d["conid"]):d for d in bundle.get("contracts", [])}
    def contract(position):
        return contracts.get(int(position["conid"]), {})
    def symbol(position):
        d = contract(position)
        bond_contracts = d.get("bond", {}).get("bondContract", [])
        labels = [h.get("n") for bc in bond_contracts for h in bc.get("hb", []) if h.get("n")]
        dated_label = next((label for label in labels if re.search(r"\d{2}/\d{2}/\d{2}",label)),None)
        return unescape(dated_label or d.get("ticker") or position.get("contractDesc", "-"))
    def description(position):
        d = contract(position)
        return symbol(position) if position.get("assetClass")=="BOND" else unescape(d.get("name") or d.get("fullName") or position.get("contractDesc", "-"))
    def instrument(position):
        return "ETF" if contract(position).get("type")=="ETF" else position.get("assetClass", "-")
    section("Open Position Summary", current_label)
    text("CURRENT POSITIONS", 388, 72, 6, right=True)
    allocation = allocation_rows("FINANCIAL_INSTRUMENT")
    table(["Financial Instrument", "Value ("+base+")", "%", "Side"], allocation,
          385, 96, [165, 90, 65, 55], 14)
    positive = [(symbol(p), finite(p.get("mktValue"))) for p in positions if p.get("currency")==base]
    positive = sorted([(s,v) for s,v in positive if v is not None and v>0], key=lambda r:r[1], reverse=True)
    if positive:
        top = positive[:5]
        if len(positive)>5:
            top.append(("Other", sum(v for _,v in positive[5:])))
        total = sum(v for _,v in top)
        angle = 90
        for n, (_, val) in enumerate(top):
            extent = 360*val/total
            c.setFillColor(HexColor(palette[n%len(palette)]))
            c.wedge(218, 612-192, 338, 612-72, angle, extent, fill=1, stroke=0)
            angle += extent
        c.setFillColor(HexColor("#FFFFFF"))
        c.circle(278, 612-132, 33, fill=1, stroke=0)
        text("Holdings only", 245, 211, 6, muted)
    widths = [140, 170, 100, 65, 60, 80, 80, 85]
    widths = [w*744/sum(widths) for w in widths]
    headers = ["Symbol", "Description", "Sector", "Quantity", "Price", "Value", "Cost Basis", "Unrealized P&L"]
    rows = []
    previous_group = None
    for p in sorted(positions, key=lambda p:(instrument(p),p.get("currency", ""),symbol(p))):
        group = (instrument(p),p.get("currency", "-"))
        if group != previous_group:
            rows.append([group[0]+" ("+group[1]+")"]+[""]*7)
            previous_group = group
        quantity, avg = finite(p.get("position")), finite(p.get("avgCost"))
        # IBKR avgCost is per unit of position (bonds use face quantity).
        cost = quantity*avg if quantity is not None and avg is not None else None
        face_unit = finite(contract(p).get("bond", {}).get("principalValue"))
        displayed_quantity = quantity*face_unit if p.get("assetClass")=="BOND" and quantity is not None and face_unit is not None else quantity
        rows.append([symbol(p),description(p),contract(p).get("sector") or "-",
                     money(displayed_quantity), money(p.get("mktPrice")),money(p.get("mktValue")),money(cost),
                     money(p.get("unrealizedPnl"))])
    if rows:
        for currency, ledger in bundle.get("ledger", {}).items():
            if currency=="BASE" or ledger.get("acctcode")!=account:
                continue
            cash = finite(ledger.get("cashbalance"))
            if cash is not None:
                rows.extend([["Cash ("+currency+")"]+[""]*7,
                             [currency,"Cash balance (excludes accruals)","Cash",money(cash),"1.00",money(cash),"-","-"]])
        end_y = table(headers, rows, 24, 244, widths, 10.8,left_columns=(0,1,2))
        if end_y < 515:
            paragraph("Values, cost and P&L retain each group's currency. Bond quantity is face value when principal units are supplied. ETFs and sectors use security definitions. Current positions are not historical PA closing holdings.", 27, end_y+8, 730, 6, muted)
    else:
        paragraph(bundle["errors"].get("positions", "No current positions returned."), 30, 260, 710)
    c.showPage()

    section("Fixed Income", current_label)
    bond_rows = bond_data(bundle)
    for r in bond_rows:
        r["name"] = symbol(r["position"])
    bonds = [r["position"] for r in bond_rows]
    grouped = defaultdict(list)
    for p in bonds:
        grouped[p.get("currency", "-")].append(p)
    rows = []
    for currency, items in sorted(grouped.items()):
        values = [finite(p.get("mktValue")) for p in items]
        faces = [r["face"] for r in bond_rows if r["currency"]==currency]
        rows.append([currency, str(len(items)),money(sum(values)) if None not in values else "-",
                     money(sum(faces)) if None not in faces else "-"])
    table(["Currency", "Bonds", "Market Value", "Face Value"], rows, 32, 84, [120,60,95,95], 22)
    text("Maturity Level", 32, 170, 11, muted)
    asof = datetime.fromisoformat(bundle["collected_at"]).date()
    buckets = {"Reserves 0-1":0.,"Short Term 1-5":0.,"Intermediate 5-10":0.,"Long Term 10+":0.}
    eligible = [r for r in bond_rows if r["currency"]==base and r["maturity"]]
    total = sum(finite(r["position"].get("mktValue")) or 0 for r in eligible)
    for r in eligible:
        years_to_maturity = (datetime.strptime(r["maturity"],"%Y%m%d").date()-asof).days/365.25
        bucket = list(buckets)[0 if years_to_maturity<=1 else 1 if years_to_maturity<=5 else 2 if years_to_maturity<=10 else 3]
        buckets[bucket] += finite(r["position"].get("mktValue")) or 0
    if total>0:
        avg_maturity = sum((finite(r["position"].get("mktValue")) or 0)*
                           (datetime.strptime(r["maturity"],"%Y%m%d").date()-asof).days/365.25
                           for r in eligible)/total
        text(f"{avg_maturity:.2f} Yr.",32,145,13,muted)
        text("Value-weighted maturity",115,145,6,muted)
    table(["Maturity Level", "Market Value", "%"], [[k,money(v),percent(v/total) if total else "-"] for k,v in buckets.items()],
          32,197,[175,100,40],16,bars=True)
    text("Credit Quality", 420, 71, 11, muted)
    ratings = defaultdict(float)
    for r in bond_rows:
        if r["currency"] != base:
            continue
        for rating in r["definition"].get("bond", {}).get("issueRating", []):
            if rating.get("agency") in ("MOODY","SP"):
                ratings[(rating["agency"],rating["ratingValue"])]+=(finite(r["position"].get("mktValue")) or 0)
    table(["Agency / Rating", "Market Value", "%"],
          [[a+" / "+r,money(v),percent(v/total) if total else "-"] for (a,r),v in sorted(ratings.items())],
          420,99,[145,100,40],16,bars=True)
    paragraph("Ratings are current issue ratings from security definitions. Each agency is a separate view; its percentages use total bond value in the base currency. Rating dates/outlooks are not inferred.",420,360,325,7,muted)
    paragraph("Duration and yield-to-maturity remain unavailable from these responses. Maturity bands use calendar days / 365.25 and current values. These calculations are not asserted equal to IBKR's PDF analytics.",32,355,345,7,muted)
    paragraph("Coupon schedules below are contractual estimates for supported fixed bullet bonds, scaled to the actual face amount. Calls, defaults, taxes and future holding changes can alter actual receipts.",32,451,700,7,muted)
    c.showPage()

    section("Fixed Income (Cont.)", current_label)
    text("Cash Flow by Month",30,69,11,muted)
    flow_rows = []
    all_flows = sorted([(f["date"],r,f) for r in bond_rows for f in r["flows"]],key=lambda item:(item[0],item[1]["name"]))
    for date,r,flow in all_flows:
        flow_rows.append([date.strftime("%b %d, %Y"),r["name"],r["currency"],money(flow["interest"]),money(flow["principal"])])
    # Two compact columns, following the source report's cash-flow layout.
    for start in range(0,max(len(flow_rows),1),64):
        if start:
            c.showPage()
            page("Fixed Income (Cont.)",current_label)
            text("Cash Flow by Month",30,69,11,muted)
        for column in range(2):
            chunk = flow_rows[start+column*32:start+(column+1)*32]
            table(["Date","Description","CCY","Interest","Principal"],chunk,
                  28+column*376,94,[65,157,25,55,55],13,left_columns=(0,1))
    if not flow_rows:
        paragraph("No supported contractual cash-flow schedule was returned.",32,150,700)
    c.showPage()

    section("Projected Income", current_label)
    income_rows = []
    for r in bond_rows:
        remaining = sum(f["interest"] for f in r["flows"] if f["date"].year==asof.year) if r["schedule_supported"] else None
        value = finite(r["position"].get("mktValue"))
        current_yield = r["annual_income"]/value if r["annual_income"] is not None and value not in (None,0) else None
        income_rows.append([r["name"],r["currency"],money(r["face"]),money(value),
                            percent(current_yield),money(r["annual_income"]),money(remaining)])
    table(["Bond","CCY","Face Value","Market Value","Current Yield","Est. Annual Income",f"Est. {asof.year} Remaining"],
          income_rows,28,100,[200,35,85,85,80,120,139],20)
    paragraph("Fixed-coupon income estimates assume current quantities and contractual payments remain unchanged; callable/defaulted securities can differ. Remaining income uses future schedule dates in the retrieval year. A partial supported set is not a complete portfolio forecast.",32,355,710,7,muted)
    paragraph("ETF and stock dividend forecasts are not returned by the queried PA/security-definition endpoints. They remain unavailable; no historical dividend is silently converted into a forecast.",32,425,710,7,muted)
    c.showPage()

    section("Deposits and Withdrawals", period_label)
    missing("Deposits and Withdrawals",
            "The PA performance, allocation and current portfolio responses do not contain the account-level cash-transfer ledger for this period. /pa/transactions is a per-contract transaction-history endpoint; it does not establish complete deposits, withdrawals or position-transfer coverage. An authoritative period cash-flow source is required.")
    c.showPage()

    section("Notes and Disclosure", "")
    notes = [
        ("Report provenance", "This PDF was generated by AGM from authenticated Interactive Brokers API responses. It is an AGM reconstruction, not an official broker-issued PortfolioAnalyst report. The supplied official PDF is used only as a visual reference and for its cover artwork/font; none of its financial figures is copied."),
        ("Performance", f"The provider returned {measure}. NAV and cumulative returns are displayed as received. Available range: {dates[0]} through {dates[-1]}; requested period {bundle['requested_period']}. The source PDF used a different date range and MWR. The two outputs are not performance-equivalent."),
        ("Valuation dates", "PA history and live portfolio holdings are different snapshots. Current positions and cash must not be presented as holdings at the PA history end date. Allocation realtime status and missing date metadata are disclosed below."),
        ("Displayed detail", "The requested fee-detail row is omitted. Actual NAV and provider returns remain unchanged; this report does not add back costs or present a gross-of-cost return."),
        ("Missing information", "Unavailable data is not zero. NAV change is not profit. Top/bottom period performers, the complete NAV-change bridge, bond duration/yield, dividend forecasts and account-level deposits/withdrawals require additional verified sources. Supported fixed-coupon bullet schedules use the returned redemption notional to scale contractual payments to actual face value; early calls are not projected."),
        ("Formatting", "Money is displayed to two decimals with half-even rounding. Returns received as decimal fractions are multiplied by 100 for percentage display. Different currencies are never summed without an authoritative conversion basis."),
        ("Access", f"Account {account}, verified master {bundle['master']}. Account Management authentication and the SSO client are separate; SSO master credential context is {bundle.get('sso_master','-')}. No orders, transfers, account updates or brokerage-session competition are part of this script."),
        ("Source receipts", f"Data retrieved at {bundle['collected_at']}. Allocation realtime={bundle.get('allocation',{}).get('realtime','unavailable')}; date={bundle.get('allocation',{}).get('date','not supplied')}. {len(bundle.get('receipts',[]))} bounded requests recorded in memory; provider responses and credentials are not saved."),
    ]
    y = 67
    for title, body in notes:
        text(title, 30, y, 8, muted)
        y = paragraph(body, 30, y+9, 725, 7)+17
    for name, error in bundle.get("errors", {}).items():
        y = paragraph(name+": "+error, 30, y, 725, 6.5)+8
    c.save()
    doc = fitz.open(stream=stream.getvalue(), filetype="pdf")
    if reference_doc:
        if reference_doc[0].get_text().strip():
            raise ReportError("Reference cover contains text; review before reusing artwork")
        doc.delete_page(0)
        doc.insert_pdf(reference_doc, from_page=0, to_page=0, start_at=0)
    # Add contents after flow pagination has established actual page numbers.
    toc = doc[1]
    for n, (title, page_number) in enumerate(contents):
        yy = 100+n*42
        toc.insert_text((42, yy), title, fontsize=10, fontname="helv", color=(.15,.15,.15))
        toc.insert_text((730, yy), str(page_number), fontsize=10, fontname="helv", color=(.15,.15,.15))
        toc.draw_line((42, yy+12),(751,yy+12),color=(.88,.88,.88),width=.4)
    result = doc.tobytes(garbage=4, deflate=True)
    doc.close()
    if reference_doc:
        reference_doc.close()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", default=DEFAULT_ACCOUNT)
    parser.add_argument("--master", default=DEFAULT_MASTER)
    parser.add_argument("--credential", default=DEFAULT_CREDENTIAL)
    parser.add_argument("--sso-master", default="I6413690", help="SSO provisioning context; distinct from account master")
    parser.add_argument("--period", choices=["1D", "7D", "MTD", "1M", "YTD", "1Y"], default="1Y")
    parser.add_argument("--ip")
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    from zoneinfo import ZoneInfo
    import os
    stamp = datetime.now(ZoneInfo("America/Costa_Rica")).strftime("%Y%m%d_%H%M%S")
    output = args.output or ROOT / "output/pdf" / f"{args.account}_PortfolioAnalyst_{args.period}_{stamp}.pdf"
    if output.exists():
        raise ReportError("Output already exists; choose another --output before contacting IBKR")
    client = PAClient(args.master, args.credential, args.sso_master)
    client.connect(args.ip)
    bundle = client.collect(args.account, args.period)
    pdf = render_report(bundle, args.reference)
    output.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(output,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,"wb") as handle:
        handle.write(pdf)
    print(f"PDF: {output}")
    if bundle["errors"]:
        print("Unavailable sections: " + ", ".join(bundle["errors"]), file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except ReportError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
