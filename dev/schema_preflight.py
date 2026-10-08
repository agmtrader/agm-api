"""Read-only candidate-image schema check, run before deployment.

Run from the repo root: python -m dev.schema_preflight
Uses existing environment/ADC, imports no Flask app and ignores DEV_MODE.
"""
from time import perf_counter

from src.utils.connectors.supabase import initialize_database


def main():
    started = perf_counter()
    manager = initialize_database()
    try:
        manager.validate_schema()
        print(f'Schema preflight passed in {perf_counter() - started:.3f}s')
    finally:
        manager.engine.dispose()


if __name__ == '__main__':
    main()
