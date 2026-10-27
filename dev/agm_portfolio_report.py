#!/usr/bin/env python3
"""AGM-designed account report from authenticated IBKR API data.

The separate portfolio_analyst.py renderer is preserved. This renderer does not
read the reference broker PDF, its artwork, its fonts or its disclosure text.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN
from html import unescape
from io import BytesIO
import os
from pathlib import Path
import sys
from xml.sax.saxutils import escape
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0,str(ROOT))
from dev.portfolio_analyst import (PAClient, ReportError, account_series, bond_data, finite,
                                  DEFAULT_ACCOUNT, DEFAULT_MASTER, DEFAULT_CREDENTIAL)

NAVY = "#062D47"
ORANGE = "#F26C0D"
BLUE = "#2571A5"
MUTED = "#646C77"
SURFACE = "#F1F5F9"
LINE = "#DCE4EB"


def money(value):
    if finite(value) is None:
        return "Unavailable"
    return f"{Decimal(str(value)).quantize(Decimal('.01'),rounding=ROUND_HALF_EVEN):,.2f}"


def pct(value):
    return "Unavailable" if finite(value) is None else f"{float(value)*100:.2f}%"


def human_date(value):
    try:
        return datetime.strptime(str(value)[:8],"%Y%m%d").strftime("%d %b %Y")
    except ValueError:
        return str(value)


def render_agm_report(bundle):
    from reportlab.pdfgen import canvas
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib.colors import HexColor
    from reportlab.platypus import Paragraph
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.pagesizes import landscape, A4
    import fitz

    fonts = Path(__file__).parent / "report_fonts"
    for name, filename in (("AGM","Rubik-Regular.ttf"),("AGM-Medium","Rubik-Medium.ttf")):
        path = fonts / filename
        if not path.is_file():
            raise ReportError("Bundled Rubik font missing: "+filename)
        pdfmetrics.registerFont(TTFont(name,str(path)))
    account, master = bundle["account"], bundle["master"]
    info, perf = bundle.get("account_info",{}), bundle["performance"]
    if info.get("accountId")!=account or info.get("masterAccountId")!=master:
        raise ReportError("Verified account/master required")
    dates, navs = account_series(perf.get("nav"),account,"navs")
    return_dates, returns = account_series(perf.get("cps"),account,"returns")
    months, month_returns = account_series(perf.get("tpps"),account,"returns")
    if dates!=return_dates or perf.get("pm") not in ("TWR","MWR"):
        raise ReportError("Performance date/measure mismatch")
    currency, measure = info.get("baseCurrency"),perf["pm"]
    for key in ("nav","cps","tpps"):
        if any(r.get("id")==account and r.get("baseCurrency")!=currency for r in perf[key]["data"]):
            raise ReportError("Performance currencies disagree")
    if perf["tpps"].get("freq")!="M":
        raise ReportError("Expected monthly period returns")
    nav_row = next(r for r in perf["nav"]["data"] if r.get("id")==account)
    positions = bundle.get("positions",[])
    definitions = {int(d["conid"]):d for d in bundle.get("contracts",[])}
    bonds = bond_data(bundle)
    asof = datetime.fromisoformat(bundle["collected_at"]).astimezone(ZoneInfo("America/Costa_Rica"))
    period = human_date(dates[0])+" - "+human_date(dates[-1])
    def definition(p):
        return definitions.get(int(p["conid"]),{})
    def name(p):
        d = definition(p)
        labels = [h.get("n","") for bc in d.get("bond",{}).get("bondContract",[]) for h in bc.get("hb",[])]
        import re
        label = next((s for s in labels if re.search(r"\d{2}/\d{2}/\d{2}",s)),None)
        return unescape(label or d.get("ticker") or p.get("contractDesc","Unavailable"))
    def description(p):
        d = definition(p)
        return name(p) if p.get("assetClass")=="BOND" else unescape(d.get("name") or name(p))
    def asset(p):
        if p.get("assetClass")=="BOND": return "Bonds"
        if definition(p).get("type")=="ETF": return "ETFs"
        return "Stocks" if p.get("assetClass")=="STK" else p.get("assetClass","Other")
    for bond in bonds:
        bond["name"] = name(bond["position"])

    output = BytesIO()
    W,H = landscape(A4)
    M = 38
    CW = W-2*M
    c = canvas.Canvas(output,pagesize=(W,H),pageCompression=1)
    c.setTitle(f"AGM Portfolio Report | {account}")
    c.setAuthor("AGM Technology")
    c.setSubject("Account information, source coverage and independently described calculations")
    logo = ROOT.parent / "agm-dashboard/public/assets/brand/agm-logo.png"
    if not logo.is_file():
        raise ReportError("AGM logo missing")

    def text(value,x,y,size=9,color=NAVY,bold=False,right=False):
        c.setFillColor(HexColor(color))
        c.setFont("AGM-Medium" if bold else "AGM",size)
        (c.drawRightString if right else c.drawString)(x,H-y,str(value))
    def rule(x,y,width=CW,color=LINE):
        c.setStrokeColor(HexColor(color));c.setLineWidth(.5)
        c.line(x,H-y,x+width,H-y)
    def panel(x,y,w,h,fill=SURFACE,stroke=None,radius=9):
        c.setFillColor(HexColor(fill))
        c.setStrokeColor(HexColor(stroke or fill))
        c.roundRect(x,H-y-h,w,h,radius,fill=1,stroke=bool(stroke))
    def paragraph(value,x,y,width,size=9,color=MUTED,leading=None):
        style=ParagraphStyle("AGMBody",fontName="AGM",fontSize=size,leading=leading or size*1.5,
                             textColor=HexColor(color))
        p=Paragraph(escape(str(value)),style)
        _,height=p.wrap(width,H)
        p.drawOn(c,x,H-y-height)
        return y+height
    def header(title,subtitle=None):
        c.drawImage(str(logo),M,H-53,width=86,height=28,preserveAspectRatio=True,mask="auto")
        text("ACCOUNT REPORT",W-M,31,8,MUTED,right=True)
        text(account+"  /  "+currency,W-M,46,8,NAVY,right=True)
        rule(M,66)
        text(title,M,104,25,bold=True)
        if subtitle: text(subtitle,M,124,8.5,MUTED)
    def footer():
        rule(M,H-35)
        text("Prepared by AGM from IBKR API data",M,H-20,7,MUTED)
        text(asof.strftime("%d %b %Y | %H:%M Costa Rica"),W-M-60,H-20,7,MUTED,right=True)
    def new_page(title,subtitle=None):
        c.showPage();header(title,subtitle)
    def card(label,value,x,y,w,detail=None,accent=False):
        panel(x,y,w,81,fill="#FFF5ED" if accent else SURFACE)
        text(label,x+16,y+22,8,MUTED)
        text(value,x+16,y+49,22,ORANGE if accent else NAVY,bold=True)
        if detail: text(detail,x+16,y+67,7.3,MUTED)
    def badge(label,x,y,w=155):
        panel(x,y,w,24,fill="#FFF3E7",stroke="#FAC49E",radius=5)
        text(label,x+9,y+16,8,"#8C430C",bold=True)
    def table(headers,rows,x,y,widths,row_height=20,size=8,left=(0,),title="Holdings (continued)",subtitle=None):
        def heading(at):
            panel(x,at,sum(widths),26,fill=NAVY,radius=4)
            for i,h in enumerate(headers):
                xx=x+sum(widths[:i]);r=i not in left
                text(h,xx+(widths[i]-8 if r else 8),at+17,7.1,"#FFFFFF",bold=True,right=r)
            return at+26
        y=heading(y)
        for n,row in enumerate(rows):
            if y+row_height>H-55:
                footer();new_page(title,subtitle)
                y=heading(145)
            if n%2==0: panel(x,y,sum(widths),row_height,fill="#F7F9FB",radius=0)
            for i,v in enumerate(row):
                xx=x+sum(widths[:i]);r=i not in left
                value=str(v);limit=widths[i]-16
                while pdfmetrics.stringWidth(value,"AGM",size)>limit and len(value)>4:
                    value=value[:-4]+"..."
                text(value,xx+(widths[i]-8 if r else 8),y+row_height*.68,size,right=r)
            y+=row_height
        return y
    def chart(title,ds,values,x,y,width,height,pct_axis=False):
        text(title,x,y,12,bold=True)
        left,top,w,h=x+54,y+25,width-65,height-65
        low,high=min(values),max(values)
        span=high-low or max(abs(high)*.05,1)
        low-=span*.12;high+=span*.12
        for k in range(5):
            val=low+(high-low)*k/4; yy=top+h-h*k/4
            rule(left,yy,w)
            text(pct(val) if pct_axis else f"{val:,.0f}",left-8,yy+3,7,MUTED,right=True)
        c.saveState()
        clip=c.beginPath();clip.rect(left,H-top-h,w,h);c.clipPath(clip,stroke=0)
        c.setStrokeColor(HexColor(ORANGE if pct_axis else BLUE));c.setLineWidth(1.35)
        p=c.beginPath()
        for n,val in enumerate(values):
            xx=left+n*w/max(len(values)-1,1);yy=H-top-h+(val-low)*h/(high-low)
            if n:p.lineTo(xx,yy)
            else:p.moveTo(xx,yy)
        c.drawPath(p);c.restoreState()
        for n in sorted({round(k*(len(ds)-1)/4) for k in range(5)}):
            text(ds[n][4:6]+"/"+ds[n][2:4],left+n*w/max(len(ds)-1,1)-10,top+h+18,7,MUTED)
    def allocation(category):
        raw=bundle.get("allocation",{})
        if raw.get("currency")!=currency:return []
        return [(item.get("name","Unavailable"),item.get("nav"),item.get("weight"),side)
                for side,data in raw.get("allocations",{}).get(category,{}).items()
                for item in data.get("items",[])]
    def bars(items,x,y,width,row_height=38):
        for label,val,weight,side in items:
            text(label+ (" / short" if side=="short" else ""),x,y,9,bold=True)
            text(money(val)+" "+currency,x+width,y,8,MUTED,right=True)
            panel(x,y+9,width-55,5,fill=LINE,radius=2)
            if finite(weight) is not None:
                panel(x,y+9,(width-55)*min(abs(float(weight)),1),5,fill=ORANGE,radius=2)
            text(pct(weight),x+width,y+15,8,right=True)
            y+=row_height
        return y

    # 1: an AGM summary, with actual period and snapshot contexts separated.
    header("Portfolio report",info.get("accountTitle") or account)
    badge("SOURCE COVERAGE: PARTIAL",W-M-195,88,195)
    text(period+"  |  "+measure+"  |  "+currency,M,151,10,MUTED)
    gap=14;card_width=(CW-gap*2)/3
    card("LATEST PERIOD NAV",money(navs[-1]),M,172,card_width,currency+" / "+human_date(dates[-1]))
    card("PERIOD RETURN",pct(returns[-1]),M+card_width+gap,172,card_width,
         "Broker-returned "+measure+" / "+bundle["requested_period"],accent=True)
    card("CURRENT POSITIONS",str(len(positions)) if "positions" in bundle else "Unavailable",
         M+(card_width+gap)*2,172,card_width,"Retrieved "+asof.strftime("%d %b %Y"))
    chart("Cumulative return",return_dates,returns,M,285,CW*.64,210,True)
    x=M+CW*.69
    text("Report scope",x,285,12,bold=True)
    y=paragraph("Historical NAV and returns are supplied by IBKR. Holdings, allocation and security definitions are retrieved separately and can reflect a different snapshot.",x,302,CW*.31,9)
    y=paragraph("AGM prepares this report and the calculations described in the methodology. It is not a broker-issued statement or an official PortfolioAnalyst report.",x,y+13,CW*.31,9)
    panel(M,512,CW,31,fill="#FFF5ED")
    text("Some report fields need additional source coverage. See Methods & coverage before interpreting totals or estimates.",M+12,532,8,"#8C430C")
    footer()

    # 2: standard financial charts with independent layout and copy.
    new_page("Performance",period+" | "+measure+" | "+currency)
    chart("Net asset value",dates,navs,M,160,(CW-24)/2,205)
    chart("Cumulative return",return_dates,returns,M+(CW+24)/2,160,(CW-24)/2,205,True)
    periods=bundle.get("allperiods",{}).get(account,{})
    text("Available return windows",M,383,12,bold=True)
    keys=[k for k in ("1D","7D","MTD","1M","YTD","1Y") if periods.get(k,{}).get("cps")]
    for n,key in enumerate(keys):
        xx=M+n*CW/6
        text(key,xx,408,8,MUTED)
        text(pct(periods[key]["cps"][-1]),xx,434,19,bold=True)
    paragraph("Window figures come from IBKR's all-periods response; the main charts use the requested-period response. Separate snapshots may differ slightly. The API measure is preserved; no MWR/TWR conversion is performed.",M,467,CW,8)
    footer()

    # 3: monthly returns, no invented quarter/year compounding.
    new_page("Monthly returns",period+" | "+measure)
    years=sorted({d[:4] for d in months},reverse=True)
    values=dict(zip(months,month_returns))
    import calendar
    rows=[[calendar.month_name[m]]+[pct(values.get(year+f"{m:02d}")) if year+f"{m:02d}" in values else "-" for year in years] for m in range(1,13)]
    table(["Month"]+years,rows,M,153,[240]+[(CW-240)/len(years)]*len(years),25,9)
    paragraph("The first and last months can be partial. A dash means no monthly value was returned for this request; it does not mean zero. AGM does not infer full-quarter or full-year returns from these partial periods.",M,500,CW,8)
    footer()

    # 4: currencies remain explicit, and source quantities are preserved.
    new_page("Current holdings","Snapshot retrieved "+asof.strftime("%d %b %Y, %H:%M Costa Rica"))
    rows=[]
    for p in sorted(positions,key=lambda p:(asset(p),p.get("currency",""),name(p))):
        quantity,avg=finite(p.get("position")),finite(p.get("avgCost"))
        face_unit=finite(definition(p).get("bond",{}).get("principalValue"))
        display_qty=quantity*face_unit if p.get("assetClass")=="BOND" and quantity is not None and face_unit is not None else quantity
        cost=quantity*avg if quantity is not None and avg is not None else None
        rows.append([name(p),asset(p),p.get("currency","-"),money(display_qty),money(p.get("mktValue")),money(cost),money(p.get("unrealizedPnl"))])
    end=table(["Holding","Type","CCY","Quantity / face","Market value","Cost basis","Unrealized P&L"],rows,M,151,
              [209,60,40,108,116,116,CW-649],18,7.8,left=(0,1,2),subtitle=asof.strftime("%d %b %Y"))
    end=paragraph("Bond face values use the security definition's principal unit; other quantities retain API position units. Cost basis is API position quantity x average cost. Each row's currency applies to all its monetary columns.",M,end+15,CW,8)
    cash=[]
    for cur,ledger in bundle.get("ledger",{}).items():
        if cur!="BASE" and ledger.get("acctcode")==account and finite(ledger.get("cashbalance")) is not None:
            cash.append(cur+" "+money(ledger["cashbalance"]))
    paragraph("Cash balance, excluding accruals: "+(" / ".join(cash) or "Unavailable")+". Holdings and cash are current reads and are not a reconstruction of historical closing holdings.",M,end+9,CW,8)
    footer()

    # 5: allocation returned by IBKR, with explicitly computed maturity bands.
    new_page("Allocation & fixed income","Current sources / "+currency+" values only where aggregated")
    text("Financial instruments",M,160,13,bold=True)
    bars(allocation("FINANCIAL_INSTRUMENT"),M,190,325)
    text("Asset classes",M,374,13,bold=True)
    bars(allocation("ASSET_CLASS"),M,403,325,row_height=34)
    x=M+CW/2+25;rw=CW/2-25
    text("Bond maturity bands",x,160,13,bold=True)
    eligible=[r for r in bonds if r["currency"]==currency and r["maturity"] and finite(r["position"].get("mktValue")) is not None]
    total=sum(r["position"]["mktValue"] for r in eligible)
    buckets={"Up to 1 year":0.,"1 to 5 years":0.,"5 to 10 years":0.,"Over 10 years":0.}
    for r in eligible:
        years_to_maturity=(datetime.strptime(r["maturity"],"%Y%m%d").date()-asof.date()).days/365.25
        label=list(buckets)[0 if years_to_maturity<=1 else 1 if years_to_maturity<=5 else 2 if years_to_maturity<=10 else 3]
        buckets[label]+=r["position"]["mktValue"]
    bars([(k,v,v/total if total else None,"long") for k,v in buckets.items()],x,190,rw)
    text("Current issue ratings",x,355,12,bold=True)
    rating_rows=[]
    for bond in sorted(bonds,key=lambda r:r["name"]):
        ratings=bond["definition"].get("bond",{}).get("issueRating",[])
        agency=lambda key:" / ".join(sorted({r["ratingValue"] for r in ratings if r.get("agency")==key and r.get("ratingValue")})) or "n/a"
        rating_rows.append([bond["name"],agency("SP"),agency("MOODY")])
    table(["Bond","S&P","Moody's"],rating_rows,x,369,[rw-110,55,55],14,7.1,left=(0,))
    paragraph("Ratings are supplied in the security definitions; rating dates were not returned. Maturity bands use calendar days / 365.25 and current values.",x,518,rw,7.3)
    footer()

    # 6: income is clearly an AGM-derived estimate, not an API guarantee.
    new_page("Bond income estimates","Contractual schedules / current holdings / excludes equity dividend forecasts")
    supported=[r for r in bonds if r["schedule_supported"]]
    by_currency=defaultdict(float)
    for r in supported:by_currency[r["currency"]]+=r["annual_income"]
    value=" / ".join(cur+" "+money(amount) for cur,amount in sorted(by_currency.items())) or "Unavailable"
    card("ESTIMATED ANNUAL COUPON RUN RATE",value,M,150,CW*.55,
         f"Supported schedules: {len(supported)} of {len(bonds)} bonds",accent=True)
    paragraph("Calculated by AGM from face amount and contractual coupon rate. These are estimates, not promised receipts. See the assumptions below.",M+CW*.59,160,CW*.41,9)
    rows=[]
    for r in bonds:
        remaining=sum(f["interest"] for f in r["flows"] if f["date"].year==asof.year) if r["schedule_supported"] else None
        val=finite(r["position"].get("mktValue"))
        yield_value=r["annual_income"]/val if r["annual_income"] is not None and val not in (None,0) else None
        rows.append([r["name"],r["currency"],money(r["face"]),pct(yield_value),money(r["annual_income"]),money(remaining)])
    end=table(["Bond","CCY","Face value","Current yield","Annual coupon","Remaining "+str(asof.year)],rows,M,255,
              [245,40,115,110,125,CW-635],20,8,left=(0,1),title="Bond income estimates (continued)")
    if end>H-115:
        footer();new_page("Income assumptions");end=145
    paragraph("Estimates assume unchanged positions and contractual payments. Only supported fixed-coupon bullet schedules are used. Early calls, default, taxes and future position changes are not predicted. Equity and ETF dividend forecasts are unavailable. Current yield is annual coupon income divided by current market value; it is not yield-to-maturity or total return.",M,end+17,CW,8.5)
    footer()

    # 7+: payment detail independently laid out, using the actual CF schedules.
    flows=sorted([(f["date"],r,f) for r in supported for f in r["flows"]],key=lambda v:(v[0],v[1]["name"]))
    for offset in range(0,max(len(flows),1),48):
        new_page("Contractual payment schedule"+(" (continued)" if offset else ""),
                 "Next three years / estimates scaled to held face amount / early calls not projected")
        for col in range(2):
            entries=flows[offset+col*24:offset+(col+1)*24]
            rows=[[d.strftime("%d %b %Y"),r["name"],r["currency"],money(f["interest"]),money(f["principal"])] for d,r,f in entries]
            w=(CW-22)/2
            table(["Date","Bond","CCY","Interest","Principal"],rows,M+col*(w+22),153,
                  [64,w-199,28,53,54],14.5,7.2,left=(0,1,2),title="Contractual payment schedule (continued)")
        footer()

    # Final page: factual, original methodology notes; no borrowed disclaimer.
    new_page("Methods & coverage","Preparation notes and information needed to interpret this report")
    col_width=(CW-30)/2
    blocks=[
        ("Who prepared this report", "AGM prepared this account summary using information obtained through Interactive Brokers APIs. Interactive Brokers did not generate or approve this PDF. The report is not an official broker statement or PortfolioAnalyst output."),
        ("Broker-supplied information", "NAV, cumulative and monthly returns, allocation, positions and security definitions are displayed from authenticated responses. The performance measure and actual date range are retained. No return series is converted between TWR and MWR."),
        ("AGM calculations", "AGM converts bond position units to face amount using the returned principal value, calculates cost basis from original quantity x average cost, groups maturities and scales supported contractual payments to the held face amount. Monetary displays use two decimals and half-even rounding."),
        ("Dates, currencies and costs", "History, allocation and current positions are separate reads and may not reconcile exactly across snapshots. Currency labels remain explicit; unlike currencies are not added together. Fee-detail lines are omitted from this presentation; supplied NAV and returns are unchanged and no fees are added back."),
        ("Coverage limits", "Complete deposits/withdrawals, the full NAV-change attribution, security-level period-return rankings, duration/YTM and equity dividend forecasts are not available from the queried sources. Their absence is not zero activity. Changes in NAV must not be read as investment profit."),
        ("Interpretation", "This report summarizes account information and estimates. It does not recommend transactions or promise future performance or income. Verify material discrepancies against broker records before relying on figures. The described projections apply only to supported contractual schedules."),
    ]
    for col in range(2):
        x=M+col*(col_width+30);y=156
        for title,body in blocks[col*3:(col+1)*3]:
            text(title,x,y,11,bold=True)
            y=paragraph(body,x,y+11,col_width,8.6)+25
    raw_allocation=bundle.get("allocation",{})
    paragraph("Source metadata: retrieved "+asof.strftime("%d %b %Y %H:%M Costa Rica")+
              "; allocation realtime="+str(raw_allocation.get("realtime","unavailable"))+
              "; allocation as-of="+str(raw_allocation.get("date","not supplied"))+".",M,505,CW,7.5)
    footer();c.save()
    document=fitz.open(stream=output.getvalue(),filetype="pdf")
    for n,p in enumerate(document):
        p.insert_text((W-M-42,H-20),f"{n+1} / {len(document)}",fontsize=7,fontname="helv",color=(.39,.42,.47))
    pdf=document.tobytes(garbage=4,deflate=True)
    document.close()
    return pdf


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account",default=DEFAULT_ACCOUNT)
    parser.add_argument("--master",default=DEFAULT_MASTER)
    parser.add_argument("--credential",default=DEFAULT_CREDENTIAL)
    parser.add_argument("--sso-master",default="I6413690")
    parser.add_argument("--period",choices=["1D","7D","MTD","1M","YTD","1Y"],default="1Y")
    parser.add_argument("--ip")
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    stamp=datetime.now(ZoneInfo("America/Costa_Rica")).strftime("%Y%m%d_%H%M%S")
    path=args.output or ROOT/"output/pdf"/f"{args.account}_AGM_Portfolio_Report_{stamp}.pdf"
    if path.exists():raise ReportError("Output exists; select another --output before contacting IBKR")
    client=PAClient(args.master,args.credential,args.sso_master)
    client.connect(args.ip)
    bundle=client.collect(args.account,args.period)
    pdf=render_agm_report(bundle)
    path.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,"wb") as handle:handle.write(pdf)
    print("PDF: "+str(path.resolve()))
    return bundle


if __name__=="__main__":
    try:main()
    except ReportError as error:
        print(str(error),file=sys.stderr)
        raise SystemExit(1)
