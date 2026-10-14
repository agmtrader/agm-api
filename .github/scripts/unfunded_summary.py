"""Render actual recipient outcomes without sending mail or making requests."""
import argparse
import html
import json
import os
from pathlib import Path


def normalize(payload):
    # The old API returns an array only after every send succeeds.
    if isinstance(payload, list):
        recipients = []
        for item in payload:
            email = item.get('email')
            sent = isinstance(email, str) and bool(email.strip())
            recipients.append({**item, 'status': 'sent' if sent else 'skipped',
                               'reason': '' if sent else 'Missing or invalid client email'})
        legacy = True
    else:
        if not isinstance(payload, dict) or not isinstance(payload.get('recipients'), list):
            raise ValueError('Expected an outcome object or legacy recipient array')
        recipients = payload['recipients']
        legacy = False
    counts = {'total': len(recipients), 'sent': 0, 'failed': 0, 'skipped': 0}
    for item in recipients:
        if item.get('status') not in ('sent', 'failed', 'skipped'):
            raise ValueError('Invalid recipient outcome')
        counts[item['status']] += 1
    status = ('partial' if counts['sent'] else 'failed') if counts['failed'] else 'success'
    if not legacy and (payload.get('status') != status or payload.get('summary') != counts):
        raise ValueError('Recipient outcomes disagree with API status/counts')
    return status, counts, recipients, legacy


def clean(value):
    if value is None:
        return '—'
    return html.escape(str(value)).replace('|', '&#124;').replace('\r', ' ').replace('\n', ' ')


def render(payload, http_status, fatal=None):
    try:
        if fatal or http_status != '200':
            raise ValueError(fatal or f'API request failed (HTTP {http_status})')
        status, counts, recipients, legacy = normalize(payload)
    except (ValueError, TypeError, AttributeError) as exc:
        detail = str(exc)
        error_id = payload.get('error_id') if isinstance(payload, dict) else None
        if error_id:
            detail += f' (error ID: {error_id})'
        message = f'Unfunded email run failed: {detail}. Recipient counts are unknown. Check API logs before retrying.'
        markdown = ('## Unfunded email run\n\n| Metric | Value |\n| --- | ---: |\n'
                    '| Status | Failed |\n| Total sent | Unknown |\n| Total failed | Unknown |\n'
                    '| Total skipped | Unknown |\n\n' + clean(message) + '\n')
        return 'failed', message, markdown
    label = {'success': 'Succeeded', 'partial': 'Completed with recipient failures',
             'failed': 'Failed — no sends confirmed'}[status]
    message = (f'Unfunded email run: {label}.\nTotal sent: {counts["sent"]}\n'
               f'Total failed: {counts["failed"]}\nTotal skipped: {counts["skipped"]}\n'
               f'Total records: {counts["total"]}')
    markdown = ('## Unfunded email run\n\n| Metric | Value |\n| --- | ---: |\n'
                f'| Status | {label} |\n' + ''.join(
                    f'| Total {"records" if key == "total" else key} | {counts[key]} |\n'
                    for key in ('sent', 'failed', 'skipped', 'total')))
    markdown += '\nSent means the Gmail send helper returned successfully; inbox delivery is not verified.\n'
    if legacy:
        markdown += '\nLegacy API response: counts inferred from the completed batch; per-recipient message IDs are unavailable.\n'
    if counts['failed']:
        message += '\nFailed sends may have been delivered. Verify delivery before retrying; no automatic batch retry was performed.'
        markdown += '\nFailed sends have uncertain delivery. Verify delivery before retrying.\n'
    markdown += ('\n<details>\n<summary>Recipient details</summary>\n\n'
                 '| Outcome | Client | IBKR account | Email | Advisor | Notice | Business days open | Reason / error | Message / error ID |\n'
                 '| --- | --- | --- | --- | --- | ---: | ---: | --- | --- |\n')
    for item in recipients:
        reason = '; '.join(str(item[key]) for key in ('reason', 'error') if item.get(key))
        values = [item.get(key) for key in ('status', 'name', 'ibkr_account_number', 'email',
                                           'advisor_email', 'notice_number', 'business_days_since_date_opened')]
        values.extend([reason, item.get('message_id') or item.get('error_id')])
        markdown += '| ' + ' | '.join(clean(value) for value in values) + ' |\n'
    markdown += '\n</details>\n'
    return status, message, markdown


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--body', type=Path)
    parser.add_argument('--http-status', default='000')
    parser.add_argument('--fatal')
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    payload = None
    fatal = args.fatal
    if args.body:
        try:
            payload = json.loads(args.body.read_text())
        except (ValueError, OSError):
            fatal = fatal or f'API returned no usable JSON (HTTP {args.http_status})'
    status, message, markdown = render(payload, args.http_status, fatal)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / 'summary.txt').write_text(message)
    (args.output_dir / 'status.txt').write_text(status)
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as stream:
        stream.write(markdown)
    print(message)
    return 1 if status == 'failed' else 0


if __name__ == '__main__':
    raise SystemExit(main())
