"""Validate scheduled API outcomes and publish bounded, non-sensitive summaries."""
import argparse
import json
import os
import sys
from pathlib import Path


# Direct Actions invocation starts in .github/scripts, outside the package root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

def count(body, key):
    value = body.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def show(value):
    return 'unavailable' if value is None else str(value)


def render(mode, body, http_status):
    if http_status != '200' or not isinstance(body, dict):
        return False, f'{mode}: request failed or returned invalid JSON (HTTP {http_status}); counts unavailable.'
    if mode == 'market':
        from src.jobs.market_result import render_market
        return render_market(body)

    skipped = body.get('screenings_skipped') is True
    lines = [f'Screenings skipped: {skipped}']
    comparison = body.get('sanctions_comparison') or {}
    lines.append('Sanctions available: ' + show(comparison.get('all_available')))
    for name in ('ofac', 'uk', 'un'):
        delta = (comparison.get('lists') or {}).get(name) or {}
        lines.append(f'{name.upper()} delta: +{show(count(delta, "added_count"))} / -{show(count(delta, "removed_count"))}')
    keys = ('total_screenings_planned', 'screenings_executed', 'contacts_targeted',
            'contacts_with_no_screenings', 'accounts_targeted', 'accounts_with_no_screenings_at_all',
            'accounts_with_some_contacts_no_screenings', 'screenings_with_list_matches',
            'screenings_without_list_matches', 'ofac_matches', 'uk_matches', 'un_matches')
    values = {key: count(body, key) for key in keys}
    # Keep the complete metrics above for validation; display a compact overview.
    display_fields = (
        ('screenings_executed', 'Screenings executed'),
        ('contacts_targeted', 'Contacts targeted'),
        ('contacts_with_no_screenings', 'Contacts with no screenings'),
        ('accounts_targeted', 'Accounts targeted'),
        ('ofac_matches', 'OFAC matches'),
        ('uk_matches', 'UK matches'),
        ('un_matches', 'UN matches'),
    )
    lines += [f'{label}: {show(values[key])}' for key, label in display_fields]
    errors = body.get('screening_errors')
    error_count = len(errors) if isinstance(errors, list) else None
    lines.append(f'Screening errors: {show(error_count)}')
    declared_errors = count(body, 'screening_errors_count')
    good = isinstance(errors, list) and not errors and declared_errors in (None, 0)
    if skipped:
        good = good and body.get('status') == 'skipped' and values['screenings_executed'] == 0
        lines.append('No new screening results; unavailable metrics must not be interpreted as zero matches.')
    else:
        good = good and body.get('status') == 'success' and all(v is not None for v in values.values())
        good = good and values['total_screenings_planned'] == values['screenings_executed'] == values['contacts_targeted']
        if values['screenings_with_list_matches'] is not None and values['screenings_without_list_matches'] is not None:
            good = good and values['screenings_with_list_matches'] + values['screenings_without_list_matches'] == values['screenings_executed']
            good = good and all(values[key] is not None and values[key] <= values['screenings_with_list_matches'] for key in ('ofac_matches', 'uk_matches', 'un_matches'))
    return bool(good), '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('market', 'screenings'))
    parser.add_argument('--body', type=Path, required=True)
    parser.add_argument('--http-status', required=True)
    args = parser.parse_args()
    try:
        body = json.loads(args.body.read_text())
    except (ValueError, OSError):
        body = None
    good, summary = render(args.mode, body, args.http_status)
    Path(os.environ['RUNNER_TEMP'], 'scheduled-summary.txt').write_text(summary)
    print(summary)
    return 0 if good else 1


if __name__ == '__main__':
    raise SystemExit(main())
