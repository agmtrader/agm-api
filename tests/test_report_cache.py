from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import Mock

import pytest

from src.utils.report_cache import ReportCache


def test_cache_expiry_copy_isolation_and_independent_reports():
    now = [0]
    cache = ReportCache(30, clock=lambda: now[0])
    load = Mock(return_value=[{'status': 'Open'}])
    cache.get('clients', load)[0]['status'] = 'mutated'
    assert cache.get('clients', load) == [{'status': 'Open'}]
    assert load.call_count == 1
    cache.get('nav', load)
    assert load.call_count == 2
    now[0] = 30
    cache.get('clients', load)
    assert load.call_count == 3


def test_errors_are_not_cached_or_hidden_by_expired_data():
    now = [0]
    cache = ReportCache(1, clock=lambda: now[0])
    cache.get('clients', lambda: ['old'])
    now[0] = 1
    load = Mock(side_effect=RuntimeError('source failed'))
    for _ in range(2):
        with pytest.raises(RuntimeError, match='source failed'):
            cache.get('clients', load)
    assert load.call_count == 2
    assert cache.get('clients', lambda: ['new']) == ['new']


def test_concurrent_misses_share_one_source_read():
    cache = ReportCache()
    started, release, joined = Event(), Event(), Event()
    def load():
        started.set()
        assert release.wait(2)
        return [{'value': 1}]
    source = Mock(side_effect=load)
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(cache.get, 'nav', source)
        assert started.wait(2)
        # Observe a waiter joining the existing Future without timing sleeps.
        original = cache.pending['nav'].result
        def wait():
            joined.set()
            return original()
        cache.pending['nav'].result = wait
        second = pool.submit(cache.get, 'nav', source)
        assert joined.wait(2)
        release.set()
        one, two = first.result(), second.result()
    assert source.call_count == 1
    one[0]['value'] = 2
    assert two == [{'value': 1}]


def test_disable_and_bound_configuration():
    cache = ReportCache(0)
    source = Mock(return_value=[])
    cache.get('clients', source)
    cache.get('clients', source)
    assert source.call_count == 2
    assert ReportCache(9999).ttl == 300
    with pytest.raises(ValueError):
        cache.get('unknown', source)


def test_reporting_readers_share_cache_but_keep_report_sources_separate():
    # Execute the actual reader functions without initializing external clients.
    import ast
    from pathlib import Path
    tree = ast.parse(Path('src/components/tools/public/reporting.py').read_text())
    names = {'get_clients_report', 'get_nav_report', '_read_daily_report'}
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    for function in functions:
        function.decorator_list = []
    drive = Mock()
    drive.get_files_in_folder.return_value = [
        {'id': 'clients-file', 'name': 'ibkr_clients.csv'},
        {'id': 'nav-file', 'name': 'ibkr_nav.csv'},
    ]
    drive.download_file.side_effect = lambda file_id, parse: [{'source': file_id}]
    namespace = {'Drive': drive, 'resources_folder_id': 'resources', '_daily_report_cache': ReportCache()}
    exec(compile(ast.Module(body=functions, type_ignores=[]), 'reporting-readers', 'exec'), namespace)
    for _ in range(2):
        assert namespace['get_clients_report']() == [{'source': 'clients-file'}]
        assert namespace['get_nav_report']() == [{'source': 'nav-file'}]
    assert drive.get_files_in_folder.call_count == 2
    assert drive.download_file.call_count == 2
