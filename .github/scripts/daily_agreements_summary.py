"""Validate a submission result and retain a compact workflow summary."""
import argparse
import json
from pathlib import Path


REQUIRED_FORMS = json.loads((Path(__file__).resolve().parents[2] / 'src/lib/ibkr_daily_agreements.json').read_text())

def render(body, http_status, master):
    if not isinstance(body, dict):
        return False, 'Submission FAILED: invalid API response. Broker acceptance is unknown; check before retrying.'
    expected = body.get('expectedForms'); results = body.get('results')
    required_count = len(REQUIRED_FORMS)
    valid = (http_status == '200' and body.get('masterAccount') == master
             and body.get('mode') == 'submission' and body.get('ok') is True
             and body.get('submitted') is True and body.get('accepted') == required_count
             and body.get('formsRequested') == required_count and body.get('documentsPrepared') == required_count
             and body.get('skipped') == 0 and body.get('rejected') == 0
             and isinstance(expected, list) and len(expected) == required_count
             and all(isinstance(n, int) for n in expected) and set(expected) == set(REQUIRED_FORMS)
             and 9902 not in expected and isinstance(results, list) and len(results) == required_count
             and all(isinstance(row, dict) for row in results))
    if valid:
        numbers = [str(row.get('formNumber')) for row in results]
        valid = (len(set(numbers)) == required_count and set(numbers) == {str(n) for n in expected}
                 and all(str(row.get('status', '')).strip().lower() in ('accepted', 'submitted', 'processed', 'success') for row in results))
    lines = [f'Agreement submission: {"PASSED" if valid else "FAILED"}',
             f'Master: {master}; HTTP: {http_status}',
             f'Request ID: {body.get("requestId") or "not returned by IBKR"}',
             f'Started: {body.get("startedAt", "unknown")}; completed: {body.get("completedAt", "unknown")}',
             f'Requested: {body.get("formsRequested", "unknown")}; prepared: {body.get("documentsPrepared", "unknown")}; skipped: {body.get("skipped", "unknown")}',
             f'All {required_count} forms have explicit successful IBKR outcomes.' if valid else 'Broker acceptance incomplete or unknown; check before retrying.']
    if isinstance(results, list):
        for row in results[:max(required_count, 100)]:
            if isinstance(row, dict):
                clean = lambda value: ' '.join(str(value or '').split())[:300]
                lines.append(f'Form {clean(row.get("formNumber"))}: {clean(row.get("status"))} {clean(row.get("message"))}')
    return bool(valid), '\n'.join(lines)


def main():
    p=argparse.ArgumentParser();p.add_argument('--body',required=True);p.add_argument('--http-status',required=True)
    p.add_argument('--master',required=True);p.add_argument('--summary',required=True);p.add_argument('--receipt');args=p.parse_args()
    try:body=json.loads(Path(args.body).read_text())
    except (ValueError,OSError):body=None
    ok,summary=render(body,args.http_status,args.master);Path(args.summary).write_text(summary+'\n')
    if args.receipt:
        # Keep a checkable receipt without credentials, PDFs or the raw provider envelope.
        fields=('masterAccount','mode','submitted','ok','formsRequested','documentsPrepared',
                'accepted','rejected','skipped','requestId','dateSubmitted','startedAt','completedAt')
        receipt={'verified':bool(ok),'httpStatus':args.http_status,'requiredForms':REQUIRED_FORMS}
        if isinstance(body,dict):
            receipt.update({key:body[key] for key in fields if key in body and isinstance(body[key],(str,int,float,bool,type(None)))})
            results=body.get('results')
            if isinstance(results,list):
                receipt['results']=[{key:row[key] for key in ('formNumber','status') if key in row and isinstance(row[key],(str,int,type(None)))} for row in results if isinstance(row,dict)]
        Path(args.receipt).write_text(json.dumps(receipt,indent=2)+'\n')
    return 0 if ok else 1

if __name__=='__main__':raise SystemExit(main())
