"""Strict market-pipeline outcome checks shared by HTTP summaries and jobs."""

EXPECTED_SOURCES = ('bonds_snapshot', 'ust_bonds_snapshot', 'stocks_snapshot', 'etfs_snapshot')

def count(body, key):
    value = body.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None

def show(value):
    return 'unavailable' if value is None else str(value)

def _render(body):
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

def render_market(body, expected_sources=None):
    if not isinstance(body, dict):
        return False, 'Market Data ETL: result unavailable.'
    try:
        good, summary = _render(body)
        if expected_sources is not None:
            steps = body.get('overview', {}).get('extract', {}).get('steps', [])
            names = [step.get('name') for step in steps]
            good = good and bool(expected_sources) and len(names) == len(expected_sources) and set(names) == set(expected_sources)
        return bool(good), summary
    except (AttributeError, KeyError, TypeError, ValueError):
        return False, 'Market Data ETL: malformed stage results; counts unavailable.'
