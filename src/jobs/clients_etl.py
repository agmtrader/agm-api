"""Run with python -m src.jobs.clients_etl; --local-details-only avoids remote writes."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import uuid


def evaluate(result, accepted=None):
    """Distinguish completed processing errors from an incomplete runner.

    The legacy accepted-coverage argument is retained for callers, but account
    and file errors no longer decide whether a completed batch exits nonzero.
    """
    stages = result.get('overview', {})
    if (result.get('status') not in ('success', 'partial')
            or any(result.get(key) is not None for key in
                   ('error', 'error_type', 'overview_publication_error', 'lock_release_error'))
            or not isinstance(stages, dict)
            or any(not isinstance(stages.get(name), dict)
                   or stages[name].get('status') not in ('success', 'partial', 'failed')
                   for name in ('extract', 'backup', 'transform'))):
        return 'failed'
    extract = stages.get('extract', {})
    # Keep the specific named-backup warning alongside raw extraction failures.
    result['warnings'] = []
    reports = stages['transform'].get('reports', [])
    for step in extract.get('steps', []):
        error = str(step.get('error') or '')
        matching = [r for r in reports if r.get('name') == step.get('name')]
        if (step.get('name') in ('742588', '734782', '732383', '794867')
                and step.get('status') == 'failed'
                and 'Error Code: 1019,' in error
                and 'Statement generation in progress' in error
                and len(matching) == 1 and matching[0].get('status') == 'success'
                and matching[0].get('backup_file')):
            result['warnings'].append({
                'name': step['name'], 'reason': 'statement_generation_in_progress',
                'message': 'New file was not generated; existing backup was published.',
                'backup_file': matching[0]['backup_file'], 'error': error})
    if (result['status'] == 'partial'
            or any(stages[name]['status'] != 'success' for name in ('extract', 'backup', 'transform'))
            or any(item.get('status') == 'failed' for item in result.get('manual_inputs', []))):
        return 'completed_with_errors'
    return 'success'


def copy_manual_inputs(etl, config, source):
    if source == etl.batch_folder_id:
        raise ValueError('Job staging must differ from the manual source folder')
    names = {c.get('batch_name') for c in config['files'] if c.get('extract_func') is None}
    # Clear only the isolated job stage, never the manual/market source folder.
    etl.Drive.clear_folder(etl.batch_folder_id)
    copied = []
    for f in etl.Drive.get_files_in_folder(source):
        if f.get('name') in names:
            outcome = {'name': f['name'], 'source_id': f['id'], 'modified_time': f.get('modifiedTime')}
            try:
                etl.Drive.service.files().copy(fileId=f['id'],
                    body={'name': f['name'], 'parents': [etl.batch_folder_id]}, supportsAllDrives=True, fields='id,name').execute()
                outcome['status'] = 'success'
            except Exception as exc:
                outcome.update(status='failed', error=type(exc).__name__)
            copied.append(outcome)
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
            if result.get('status') == 'success' and any(item['status'] == 'failed' for item in copies):
                result['status'] = 'partial'
    except Exception as exc:
        result = {'status': 'failed', 'error_type': type(exc).__name__,
                  'error_code': getattr(exc, 'code', None), 'error_id': getattr(exc, 'error_id', None)}
        from src.utils.logger import logger
        logger.error(f'Clients runner failed: {type(exc).__name__}')
    result.update(runner_started_at=started, run_id=run_id)
    # A details-only diagnostic has no three-stage pipeline overview.
    operational = (('success' if result.get('status') == 'success' else 'failed')
                   if args.local_details_only else evaluate(result, accepted))
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
    return 0 if result['operational_status'] in ('success', 'completed_with_errors') else 1


if __name__ == '__main__':
    raise SystemExit(main())
