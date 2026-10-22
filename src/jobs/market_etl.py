"""Market Cloud Run entry point; no Flask request and no whole-pipeline retries."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import uuid

from src.jobs.market_result import EXPECTED_SOURCES, render_market
from src.jobs.storage import RunStorage

SHARED_BATCH = '1N3LwrG7IossvCrrrFufWMb26VOcRxhi8'
CLIENTS_BATCH = '1EIshGq22oFg-WJ9aHdS37Heh5KaqN2ct'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--storage-bucket', required=True)
    args = parser.parse_args(argv)
    args.output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = args.output_dir / 'overview.json'
    if path.exists():
        parser.error('Output directory already contains an overview')
    started = datetime.now(timezone.utc).isoformat()
    run_id = os.getenv('CLOUD_RUN_EXECUTION') or 'agm-market-etl-' + uuid.uuid4().hex
    result = {'status': 'failed', 'operational_status': 'failed', 'pipeline': 'market_data',
              'run_id': run_id, 'runner_started_at': started}
    storage, generation = None, None
    lock = 'locks/market-etl.json'
    try:
        storage = RunStorage(args.storage_bucket)
        generation = storage.put(lock, {'run_id': run_id, 'started_at': started})
        stage = os.environ.get('ETL_BATCH_FOLDER_ID')
        if not stage or stage in (SHARED_BATCH, CLIENTS_BATCH):
            raise ValueError('A separate market staging folder is required')
        from src.components.tools.private import etl
        if etl.batch_folder_id != stage:
            raise ValueError('ETL imported with a different staging folder')
        config = etl._get_etl_config_by_name('market_data')
        names = [file['name'] for file in config['files']]
        if set(names) != set(EXPECTED_SOURCES) or len(names) != len(EXPECTED_SOURCES) or any(not file.get('extract_func') for file in config['files']):
            raise ValueError('Market source configuration is incomplete')
        # Enable the existing five-year price-return calculation in this job
        # without changing DEV_MODE or the shared configuration templates.
        for file in config['files']:
            if file['name'] in ('stocks_snapshot', 'etfs_snapshot'):
                file['historical_yields'] = True
        # A killed/uncertain prior run requires review, never blind deletion/replay.
        if etl.Drive.get_files_in_folder(stage):
            raise ValueError('Market staging is not empty; inspect the previous execution')
        from src.utils.connectors.supabase import initialize_database
        initialize_database()
        result.update(etl.run_pipeline(config))
        result['expected_sources'] = names
        good, _ = render_market(result, names)
        result['operational_status'] = 'success' if good else 'failed'
    except Exception as exc:
        result.update(operational_status='failed', error_type=type(exc).__name__)
        from src.utils.logger import logger
        logger.error(f'Market runner failed: {type(exc).__name__}')
    finally:
        if generation is not None:
            try:
                storage.delete(lock, generation)
            except Exception as exc:
                result.update(lock_release_error=type(exc).__name__, operational_status='failed')
        result['runner_completed_at'] = datetime.now(timezone.utc).isoformat()
        # Publish after release so its failure cannot be omitted from durable evidence.
        if storage:
            try:
                storage.put(f'runs/{run_id}/overview.json', result)
            except Exception as exc:
                result.update(overview_publication_error=type(exc).__name__, operational_status='failed')
        with os.fdopen(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as stream:
            json.dump(result, stream, indent=2, default=str)
    print(json.dumps({key: result.get(key) for key in ('pipeline', 'run_id', 'status', 'operational_status')}))
    return 0 if result['operational_status'] == 'success' else 1


if __name__ == '__main__':
    raise SystemExit(main())
