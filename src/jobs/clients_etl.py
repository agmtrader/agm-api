"""Run with python -m src.jobs.clients_etl; --local-details-only avoids remote writes."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--local-details-only', action='store_true')
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.limit < 0 or (args.limit and not args.local_details_only):
        parser.error('--limit requires --local-details-only and must be nonnegative')
    args.output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Each execution must have its own directory; protect local outputs.
    overview_path = args.output_dir / 'overview.json'
    if overview_path.exists():
        parser.error('Output directory already contains an overview')
    from src.components.tools.private import etl
    started = datetime.now(timezone.utc).isoformat()
    try:
        from src.utils.connectors.supabase import initialize_database
        initialize_database()
        if args.local_details_only:
            config = etl._get_file_config('account_details', [etl._get_etl_config_by_name('clients')])
            config.update(local_output_dir=str(args.output_dir), refresh_limit=args.limit)
            result = etl.extract_account_details_backup(config)
        else:
            result = etl.run_pipeline('clients')
    except Exception as exc:
        result = {'status': 'failed', 'error_type': type(exc).__name__,
                  'error_code': getattr(exc, 'code', None), 'error_id': getattr(exc, 'error_id', None)}
        from src.utils.logger import logger
        logger.error(f'Clients runner failed: {type(exc).__name__}')
    result['runner_started_at'] = started
    with os.fdopen(os.open(overview_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as stream:
        json.dump(result, stream, indent=2, default=str)
    print(json.dumps({'status': result['status'], 'overview_file': str(overview_path),
                      'summary': result.get('summary'), 'seconds': result.get('seconds')}))
    return 0 if result.get('status') == 'success' else 1


if __name__ == '__main__':
    raise SystemExit(main())
