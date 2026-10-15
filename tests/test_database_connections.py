"""Application pool configuration and real concurrent transaction lifecycles."""
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import QueuePool

from src.utils.connectors.supabase import Supabase, application_database_url
from src.utils.exception import ServiceError
from src.utils.managers.database_manager import DatabaseManager


@pytest.fixture(autouse=True)
def clean_pool_config(monkeypatch):
    for name in ('DB_POOL_MODE', 'DB_POOL_SIZE', 'DB_MAX_OVERFLOW', 'DB_POOL_TIMEOUT', 'DEV_MODE'):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize('source', ['environment', 'secret'])
def test_application_url_uses_transaction_pool_preserving_credentials_and_query(monkeypatch, source):
    url = URL.create('postgresql', username='postgres.synthetic', password='p@ss:/?#%',
                     host='aws-0-us-west-1.pooler.supabase.com', port=5432,
                     database='postgres', query={'sslmode': 'require', 'gssencmode': 'disable'})
    value = url.render_as_string(hide_password=False)
    if source == 'environment':
        monkeypatch.setenv('SUPABASE_DB_URL', value)
    else:
        monkeypatch.delenv('SUPABASE_DB_URL', raising=False)
    # No connector initialization or provider request in these unit tests.
    with patch('src.utils.connectors.supabase.get_secret', return_value=value):
        resolved = Supabase._resolve_db_url(object())
    actual = application_database_url(resolved)
    assert actual == url.set(port=6543)
    assert actual.password == 'p@ss:/?#%'


@pytest.mark.parametrize('mode,port', [('transaction', 6543), ('session', 5432), ('preserve', 5432)])
def test_explicit_maintenance_mode(monkeypatch, mode, port):
    monkeypatch.setenv('DB_POOL_MODE', mode)
    assert application_database_url('postgresql://user:pass@aws-0-us-west-1.pooler.supabase.com:5432/postgres').port == port


@pytest.mark.parametrize('value', ['postgresql://user:pass@db.synthetic.supabase.co:5432/postgres',
                                  'postgresql://user:pass@localhost:5433/fixture'])
def test_custom_and_direct_urls_are_preserved(value):
    assert application_database_url(value).render_as_string(hide_password=False) == value


@pytest.mark.parametrize('env', [{'DB_POOL_MODE': 'typo'}, {'DB_POOL_SIZE': '0'},
                                 {'DB_MAX_OVERFLOW': '-1'}, {'DB_POOL_TIMEOUT': '0'}])
def test_unbounded_or_invalid_configuration_fails_before_connecting(monkeypatch, env):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        connection = object.__new__(Supabase)
        connection.db_url = application_database_url('postgresql://user:pass@localhost/fixture')
        connection._create_engine()


@pytest.mark.parametrize('dev,size', [('true', 1), ('false', 3)])
def test_runtime_engine_pool_is_bounded_and_identifiable(monkeypatch, dev, size):
    monkeypatch.setenv('DEV_MODE', dev)
    connector = object.__new__(Supabase)
    connector.db_url = application_database_url('postgresql://user:pass@aws-0-us-west-1.pooler.supabase.com:5432/postgres')
    engine = connector._create_engine()
    # create_connect_args excludes create_engine(connect_args), inspect do_connect
    # without a network request to check the final DBAPI arguments.
    captured = {}
    @event.listens_for(engine, 'do_connect')
    def capture(dialect, record, cargs, cparams):
        captured.update(cparams)
        raise RuntimeError('stop before network')
    try:
        assert engine.pool.size() == size
        assert engine.pool._max_overflow == 0
        with pytest.raises(RuntimeError, match='stop before network'):
            engine.connect()
        assert captured['port'] == 6543
        assert captured['sslmode'] == 'require'
        assert captured['application_name'].startswith('agm:')
    finally:
        engine.dispose()


@pytest.fixture
def database(tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path}/connections.sqlite', poolclass=QueuePool,
                           pool_size=2, max_overflow=0, pool_timeout=0.05,
                           connect_args={'check_same_thread': False})
    manager = DatabaseManager(declarative_base(), engine)
    yield manager
    engine.dispose()


def test_concurrent_capacity_failures_keep_one_bounded_pool_without_retries(database):
    engine = database.engine
    original_pool = engine.pool
    lock = Lock()
    counts = {'active': 0, 'peak': 0, 'attempts': 0}
    @event.listens_for(engine, 'checkout')
    def checkout(*args):
        with lock:
            counts['active'] += 1
            counts['peak'] = max(counts['peak'], counts['active'])
    @event.listens_for(engine, 'checkin')
    def checkin(*args):
        with lock:
            counts['active'] -= 1
    @database.with_session(commit=False, delay=0)
    def unavailable(session):
        with lock:
            counts['attempts'] += 1
        session.execute(text('SELECT 1'))
        raise OperationalError('SELECT 1', {}, Exception('EMAXCONNSESSION max clients reached'))
    def request(_):
        try:
            unavailable()
        except ServiceError as exc:
            return exc.status_code
    with ThreadPoolExecutor(8) as executor:
        assert list(executor.map(request, range(24))) == [503] * 24
    assert counts['active'] == 0
    assert counts['attempts'] == 24
    assert counts['peak'] <= 2
    assert engine.pool is original_pool
    assert engine.pool.checkedout() == 0
    with engine.connect() as connection:
        assert connection.scalar(text('SELECT 1')) == 1


def test_local_checkout_timeout_is_503_without_pool_replacement(database):
    original_pool = database.engine.pool
    @database.with_session(commit=False, delay=0)
    def read(session):
        return session.scalar(text('SELECT 1'))
    with database.engine.connect(), database.engine.connect():
        with pytest.raises(ServiceError) as failure:
            read()
    assert failure.value.status_code == 503
    assert failure.value.code == 'database_pool_exhausted'
    assert database.engine.pool is original_pool
    assert read() == 1


def test_transient_read_recovers_and_returns_connections_without_disposal(database):
    pool = database.engine.pool
    attempts = []
    @database.with_session(commit=False, delay=0)
    def read(session):
        attempts.append(1)
        result = session.scalar(text('SELECT 1'))
        if len(attempts) == 1:
            raise OperationalError('SELECT 1', {}, Exception('server closed the connection'))
        return result
    assert read() == 1
    assert len(attempts) == 2
    assert database.engine.pool is pool
    assert pool.checkedout() == 0


def test_uncertain_write_is_not_replayed(database):
    with database.engine.begin() as connection:
        connection.execute(text('CREATE TABLE operations (id INTEGER)'))
    @database.with_session(delay=0)
    def write(session):
        session.execute(text('INSERT INTO operations VALUES (1)'))
        session.commit()  # Simulate an acknowledged DB commit with a lost reply.
        raise OperationalError('COMMIT', {}, Exception('server closed the connection'))
    with pytest.raises(ServiceError) as failure:
        write()
    assert failure.value.status_code == 503
    with database.engine.connect() as connection:
        assert connection.scalar(text('SELECT count(*) FROM operations')) == 1
