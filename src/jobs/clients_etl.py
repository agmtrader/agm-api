"""Run with python -m src.jobs.clients_etl; --local-details-only avoids remote writes."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import uuid


def evaluate(result, accepted):
    """Accept only explicitly recorded coverage gaps; preserve raw partial overview."""
    if result.get('status') == 'success':
        return 'success'
    stages = result.get('overview', {})
    if not stages or any(stages.get(name, {}).get('status') != 'success' for name in ('backup', 'transform')):
        return 'failed'
    extract = stages.get('extract', {})
    detail_steps = [s for s in extract.get('steps', []) if s.get('name') == 'account_details']
    if len(detail_steps) != 1 or any(s.get('status') not in ('success', 'skipped')
        for s in extract.get('steps', []) if s.get('name') != 'account_details'):
        return 'failed'
    details = detail_steps[0]
    failures = [o for o in details.get('outcomes', []) if o.get('status') == 'failed']
    expected = {(o['account_id'], o['error']) for o in accepted}
    if details.get('summary', {}).get('successful', 0) <= 0 or not failures:
        return 'failed'
    if all((o.get('account_id'), o.get('error')) in expected for o in failures):
        return 'completed_with_accepted_coverage_gaps'
    return 'failed'


def copy_manual_inputs(etl, config, source):
    if source == etl.batch_folder_id:
        raise ValueError('Job staging must differ from the manual source folder')
    names = {c.get('batch_name') for c in config['files'] if c.get('extract_func') is None}
    # Clear only the isolated job stage, never the manual/market source folder.
    etl.Drive.clear_folder(etl.batch_folder_id)
    copied = []
    for f in etl.Drive.get_files_in_folder(source):
        if f.get('name') in names:
            etl.Drive.service.files().copy(fileId=f['id'],
                body={'name': f['name'], 'parents': [etl.batch_folder_id]}, supportsAllDrives=True, fields='id,name').execute()
            copied.append({'name': f['name'], 'source_id': f['id'], 'modified_time': f.get('modifiedTime')})
    return copied


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--local-details-only', action='store_true')
    parser.add_argument('--limit', type=int, default=0)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--storage-bucket')
    parser.add_argument('--accepted-coverage-file', type=Path)
    args = parser.parse_args(argv)
    if args.limit < 0 or (args.limit and not args.local_details_only):
        parser.error('--limit requires --local-details-only and must be nonnegative')
    if args.local_details_only and args.storage_bucket:
        parser.error('Local read-only mode cannot publish to GCS')
    args.output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    overview_path = args.output_dir / 'overview.json'
    if overview_path.exists():
        parser.error('Output directory already contains an overview')
    accepted = json.loads(args.accepted_coverage_file.read_text()) if args.accepted_coverage_file else []
    from src.components.tools.private import etl
    started = datetime.now(timezone.utc).isoformat()
    run_id = os.getenv('CLOUD_RUN_EXECUTION') or uuid.uuid4().hex
    storage = None
    generation = None
    result = {'status': 'failed'}
    try:
        if args.storage_bucket:
            from src.jobs.storage import RunStorage
            storage = RunStorage(args.storage_bucket)
            generation = storage.put('locks/clients-etl.json', {'run_id': run_id, 'started_at': started})
        from src.utils.connectors.supabase import initialize_database
        initialize_database()
        if args.local_details_only:
            config = etl._get_file_config('account_details', [etl._get_etl_config_by_name('clients')])
            config.update(local_output_dir=str(args.output_dir), refresh_limit=args.limit)
            result = etl.extract_account_details_backup(config)
        else:
            source = os.getenv('CLIENTS_MANUAL_SOURCE_FOLDER_ID')
            copies = copy_manual_inputs(etl, etl._get_etl_config_by_name('clients'), source) if source else []
            result = etl.run_pipeline('clients')
            result['manual_inputs'] = copies
    except Exception as exc:
        result = {'status': 'failed', 'error_type': type(exc).__name__,
                  'error_code': getattr(exc, 'code', None), 'error_id': getattr(exc, 'error_id', None)}
        from src.utils.logger import logger
        logger.error(f'Clients runner failed: {type(exc).__name__}')
    result.update(runner_started_at=started, run_id=run_id)
    operational = evaluate(result, accepted)
    result['operational_status'] = operational
    if storage:
        try:
            storage.put(f'runs/{run_id}/overview.json', result)
            print(f'ETL_OVERVIEW_URI=gs://{args.storage_bucket}/runs/{run_id}/overview.json')
        except Exception as exc:
            result['overview_publication_error'] = type(exc).__name__
            result['operational_status'] = 'failed'
        finally:
            if generation is not None:
                try:
                    storage.delete('locks/clients-etl.json', generation)
                except Exception as exc:
                    result['lock_release_error'] = type(exc).__name__
                    result['operational_status'] = 'failed'
    with os.fdopen(os.open(overview_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as stream:
        json.dump(result, stream, indent=2, default=str)
    print(json.dumps({'status': result['status'], 'operational_status': result['operational_status'],
                      'overview_file': str(overview_path), 'run_id': run_id,
                      'summary': result.get('summary'), 'seconds': result.get('seconds')}))
    return 0 if result['operational_status'] in ('success', 'completed_with_accepted_coverage_gaps') else 1


if __name__ == '__main__':
    raise SystemExit(main())
