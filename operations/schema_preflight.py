"""Read-only candidate-image schema check, run before deployment.

Run from the repo root: python -m operations.schema_preflight
Uses existing environment/ADC, imports no Flask app and ignores DEV_MODE.
"""
from time import perf_counter
from sqlalchemy import inspect

from src.utils.connectors.supabase import initialize_database


def main():
    started = perf_counter()
    manager = initialize_database()
    try:
        manager.validate_schema()
        required = {'id', 'account_id', 'transaction_id', 'status', 'event_type', 'parent_event_id',
                    'notification_status', 'note', 'follow_up_date', 'actor_id', 'actor_email',
                    'created_at', 'transaction_snapshot'}
        columns = {column['name'] for column in inspect(manager.engine).get_columns('transaction_review_event', schema='agm_compliance')}
        if columns != required:
            raise RuntimeError(f'Transaction review schema mismatch: missing={sorted(required - columns)}, unexpected={sorted(columns - required)}')
        print(f'Schema preflight passed in {perf_counter() - started:.3f}s')
    finally:
        manager.engine.dispose()


if __name__ == '__main__':
    main()
