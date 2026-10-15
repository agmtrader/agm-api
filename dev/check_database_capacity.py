"""Read-only production + development transaction-pool smoke test.

Holds 35 client connections (10 API pools x 3, two dev pools x 1, one job x 3)
while running at most four SELECT 1 queries concurrently. No customer rows,
DDL, persistent changes, or provider calls beyond the database/secret read.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from time import monotonic

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from src.utils.connectors.supabase import Supabase, application_database_url


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--clients', type=int, default=35)
    parser.add_argument('--workers', type=int, default=4)
    args = parser.parse_args()
    if not 1 <= args.workers <= 4 or not 1 <= args.clients <= 40:
        parser.error('clients must be 1..40 and workers 1..4')
    # Resolve the same endpoint as API/jobs without initializing services.
    url = application_database_url(Supabase._resolve_db_url(object()))
    if url.port != 6543 or not (url.host or '').endswith('.pooler.supabase.com'):
        raise RuntimeError('Capacity probe requires the shared transaction pooler on port 6543')
    engine = create_engine(url, pool_size=args.clients, max_overflow=0,
                           pool_pre_ping=True, connect_args={
                               'sslmode': 'require', 'gssencmode': 'disable',
                               'connect_timeout': 10, 'application_name': f'agm:capacity-check:{os.getpid()}',
                           })
    connections = []
    started = monotonic()
    try:
        for _ in range(args.clients):
            connection = engine.connect()
            connections.append(connection)
            assert connection.scalar(text('SELECT 1')) == 1
            connection.rollback()  # Hold the client socket, release the backend.
        def read(connection):
            try:
                return connection.scalar(text('SELECT 1'))
            finally:
                connection.rollback()
        with ThreadPoolExecutor(args.workers) as executor:
            results = list(executor.map(read, connections))
        assert results == [1] * args.clients
        assert not any(connection.in_transaction() for connection in connections)
        print(json.dumps({'ok': True, 'port': make_url(url).port,
                          'held_client_connections': len(connections), 'query_workers': args.workers,
                          'successful_reads': len(results), 'open_transactions': 0,
                          'duration_seconds': round(monotonic() - started, 3)}))
    finally:
        for connection in connections:
            connection.close()
        engine.dispose()


if __name__ == '__main__':
    main()
