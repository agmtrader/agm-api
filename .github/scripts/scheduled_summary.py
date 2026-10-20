"""Validate scheduled API outcomes and publish bounded, non-sensitive summaries."""
import argparse
import json
import os
from pathlib import Path


def count(body, key):
    value = body.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def show(value):
    return 'unavailable' if value is None else str(value)


def render(mode, body, http_status):
    if http_status != '200' or not isinstance(body, dict):
        return False, f'{mode}: request failed or returned invalid JSON (HTTP {http_status}); counts unavailable.'
    if mode == 'market':
        lines = [f'Market Data ETL status: {body.get("status") if body.get("status") in ("success", "partial", "failed") else "unavailable"}']
        good = body.get('status') == 'success'
        overview = body.get('overview')
        if not isinstance(overview, dict):
            return False, '\n'.join(lines + ['Stage results unavailable.'])
        for name, child in [('extract', 'steps'), ('backup', 'steps'), ('transform', 'reports')]:
            stage = overview.get(name)
            if not isinstance(stage, dict):
                good = False
                lines.append(f'{name}: unavailable')
                continue
            state = stage.get('status')
            good = good and state == 'success'
            summary = stage.get('summary', {})
            lines.append(f'{name}: {state if state in ("success", "partial", "failed") else "unavailable"}')
            if name != 'backup':
                keys = ('total', 'successful', 'failed') + (('skipped',) if name == 'extract' else ())
                metrics = {key: count(summary, key) for key in keys} if isinstance(summary, dict) else {}
                good = good and all(metrics.get(k) is not None for k in keys)
                good = good and metrics.get('failed') == 0 and metrics.get('total', 0) > 0
                if all(metrics.get(k) is not None for k in keys):
                    good = good and metrics['total'] == metrics['successful'] + metrics['failed'] + metrics.get('skipped', 0)
                    good = good and metrics['successful'] == metrics['total']
                lines.append('  ' + ', '.join(f'{key}: {show(metrics.get(key))}' for key in keys))
            items = stage.get(child)
            if not isinstance(items, list) or not items:
                good = False
            else:
                good = good and all(isinstance(item, dict) and item.get('status') == 'success' for item in items)
                if name != 'backup' and isinstance(summary, dict):
                    good = good and len(items) == count(summary, 'total')
                if name == 'extract':
                    good = good and all(item.get('source_file_id') for item in items)
                if name == 'transform':
                    good = good and all(item.get('source_file_id') for item in items)
        extracted = (overview.get('extract') or {}).get('steps', [])
        transformed = (overview.get('transform') or {}).get('reports', [])
        if isinstance(extracted, list) and isinstance(transformed, list):
            source_ids = {item.get('name'): item.get('source_file_id') for item in extracted if isinstance(item, dict)}
            transformed_names = [item.get('name') for item in transformed if isinstance(item, dict)]
            good = good and len(source_ids) == len(extracted) == len(transformed) == len(set(transformed_names))
            good = good and set(source_ids) == set(transformed_names)
            good = good and all(isinstance(item, dict) and item.get('source_file_id') == source_ids.get(item.get('name')) for item in transformed)
        lines.append('Failure detail: review API logs and the retained overview; raw provider payloads are omitted.')
        return bool(good), '\n'.join(lines)

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
    lines += [f'{key}: {show(value)}' for key, value in values.items()]
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
