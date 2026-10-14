"""Validate a preparation result and retain a compact workflow summary."""
import argparse
import json
from pathlib import Path


def render(body, http_status, master):
    if not isinstance(body, dict):
        return False, 'Preparation FAILED: invalid API response. Nothing submitted by this workflow.'
    expected = body.get('expectedForms'); results = body.get('results')
    valid = (http_status == '200' and body.get('masterAccount') == master
             and body.get('mode') == 'preparation' and body.get('ready') is True
             and body.get('submitted') is False and body.get('accepted') == 0
             and body.get('formsRequested') == 24 and body.get('documentsPrepared') == 24
             and body.get('skipped') == 0 and body.get('rejected') == 0
             and isinstance(expected, list) and len(expected) == 24
             and all(isinstance(n, int) for n in expected) and len(set(expected)) == 24
             and 9902 not in expected and isinstance(results, list) and len(results) == 24
             and all(isinstance(row, dict) for row in results))
    if valid:
        numbers = [str(row.get('formNumber')) for row in results]
        valid = (len(set(numbers)) == 24 and set(numbers) == {str(n) for n in expected}
                 and all(row.get('status') == 'Prepared' for row in results))
    lines = [f'Agreement preparation: {"PASSED" if valid else "FAILED"}',
             f'Master: {master}; HTTP: {http_status}',
             f'Requested: {body.get("formsRequested", "unknown")}; prepared: {body.get("documentsPrepared", "unknown")}; skipped: {body.get("skipped", "unknown")}',
             'Nothing submitted to IBKR; no documents signed. This is not broker acceptance.']
    if isinstance(results, list):
        for row in results[:24]:
            if isinstance(row, dict):
                clean = lambda value: ' '.join(str(value or '').split())[:300]
                lines.append(f'Form {clean(row.get("formNumber"))}: {clean(row.get("status"))} {clean(row.get("message"))}')
    return bool(valid), '\n'.join(lines)


def main():
    p=argparse.ArgumentParser();p.add_argument('--body',required=True);p.add_argument('--http-status',required=True)
    p.add_argument('--master',required=True);p.add_argument('--summary',required=True);args=p.parse_args()
    try:body=json.loads(Path(args.body).read_text())
    except (ValueError,OSError):body=None
    ok,summary=render(body,args.http_status,args.master);Path(args.summary).write_text(summary+'\n')
    return 0 if ok else 1

if __name__=='__main__':raise SystemExit(main())
