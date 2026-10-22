"""Observed mid-extract 401 and accounts-bootstrap 500 responses; no provider calls."""
from contextlib import contextmanager
from threading import RLock
from unittest.mock import Mock

import pytest

from src.utils.connectors.ibkr_trading_api import IBKRTradingAPI
from src.utils.exception import ServiceError
from src.components.tools.private import etl


def response(payload, status=200):
    return Mock(status_code=status, json=lambda: payload, text=str(payload), headers={})


@pytest.fixture
def client(monkeypatch):
    client = IBKRTradingAPI.__new__(IBKRTradingAPI)
    client.BASE_URL = 'https://api.ibkr.com'
    client.sso_token = None
    client.CLIENT_ID = client.KEY_ID = client.CLIENT_PRIVATE_KEY = 'synthetic'
    client._credential_lock = RLock()
    monkeypatch.setattr(client, '_apply_credentials', lambda _: ('synthetic',) * 3)
    sessions = []
    def create(**kwargs):
        client.sso_token = 'sso-' + str(len(sessions))
        sessions.append(client.sso_token)
    monkeypatch.setattr(client, 'create_sso_session', create)
    monkeypatch.setattr(client, 'logout_of_brokerage_session', Mock())
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.time.sleep', lambda _: None)
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.requests.post',
                        lambda *args, **kwargs: response({'authenticated': True, 'connected': True, 'competing': False}))
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.requests.get',
                        lambda *args, **kwargs: response({'accounts': ['I1'], 'selectedAccount': 'I1', 'aliases': {}}))
    client.sessions = sessions
    return client


@pytest.mark.parametrize('status,payload', [(401, {}), (500, {'error': 'Please query /accounts first'})])
def test_lost_session_read_recovers_once_and_retains_warning(client, monkeypatch, status, payload):
    calls = []
    results = iter([response(payload, status), response([{'conid': 101}])])
    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        return next(results)
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.requests.request', request)
    with client.market_data_session():
        assert client.get_securities_by_symbol('SYNTHETIC', 'STK') == [{'conid': 101}]
        assert len(client._market_data_context['recoveries']) == 1
        assert client._market_data_context['failures'][0]['auth_status'] == {
            'http_status': 200, 'authenticated': True, 'connected': True, 'competing': False}
    assert len(calls) == 2
    assert calls[0][2]['timeout'] == (10, 45)
    assert calls[-1][2]['headers']['Authorization'] == 'Bearer ' + client.sessions[-1]
    client.logout_of_brokerage_session.assert_called_once()
    assert client.sso_token is None


def test_persistent_session_failure_is_not_hidden_or_replayed_forever(client, monkeypatch):
    request = Mock(return_value=response({'error': 'Please query /accounts first'}, 500))
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.requests.request', request)
    with pytest.raises(ServiceError) as error:
        with client.market_data_session():
            client.get_securities_by_symbol('SYNTHETIC', 'STK')
    assert error.value.code == 'ibkr_session_not_ready'
    assert request.call_count == 2
    client.logout_of_brokerage_session.assert_called_once()


def test_market_session_waits_for_documented_settlement_before_accounts(client, monkeypatch):
    events = []
    monkeypatch.setattr(client, 'initialize_brokerage_session', lambda: events.append('initialize'))
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.time.sleep', lambda seconds: events.append(('sleep', seconds)))
    def accounts():
        assert events[-2:] == ['initialize', ('sleep', 2)]
        return {'selectedAccount': 'I1'}
    monkeypatch.setattr(client, 'get_brokerage_accounts', accounts)
    with client.market_data_session():
        pass


@pytest.mark.parametrize('status,payload', [(403, {}), (500, {'error': 'different upstream failure'})])
def test_other_provider_failures_are_not_session_retries(client, monkeypatch, status, payload):
    request = Mock(return_value=response(payload, status))
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.requests.request', request)
    with pytest.raises(ServiceError):
        with client.market_data_session():
            client.get_securities_by_symbol('SYNTHETIC', 'STK')
    assert request.call_count == 1
    assert len(client.sessions) == 1


def test_expired_session_stops_symbol_resolution_instead_of_omitting_tickers(monkeypatch):
    api = Mock()
    api.get_securities_by_symbol.side_effect = ServiceError('expired', 409, code='ibkr_session_expired')
    with pytest.raises(ServiceError, match='expired'):
        etl._resolve_snapshot_conids(api, ['FIRST', 'SECOND'])
    assert api.get_securities_by_symbol.call_count == 1


def test_read_only_recovery_never_accepts_order_endpoint(client, monkeypatch):
    request = Mock()
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.requests.request', request)
    with client.market_data_session(), pytest.raises(ValueError):
        client._request_market_data_json('POST', client.BASE_URL + '/v1/api/iserver/account/I1/orders')
    request.assert_not_called()


def test_recovery_budget_is_shared_by_all_reads_in_one_source(client, monkeypatch):
    request = Mock(side_effect=[response({}, 401), response([]), response({}, 401)])
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.requests.request', request)
    with client.market_data_session():
        client.get_securities_by_symbol('FIRST', 'STK')
        with pytest.raises(ServiceError):
            client.get_securities_by_symbol('SECOND', 'STK')
        assert len(client._market_data_context['recoveries']) == 1
    assert request.call_count == 3 and len(client.sessions) == 2


def test_history_recovery_retains_pacing_and_does_not_replay_other_reads(client, monkeypatch):
    sleeps = []
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.time.sleep', sleeps.append)
    request = Mock(side_effect=[response({}, 401), response({'data': []})])
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.requests.request', request)
    with client.market_data_session():
        assert client.get_historical_market_data('101', '5y', '1d') == {'data': []}
    assert sleeps == [2, 2, 1.25] and request.call_count == 2


def test_snapshot_read_recovers_after_warmup_with_fields_and_new_token(client, monkeypatch):
    request = Mock(side_effect=[response([]), response({}, 401), response([{'55': 'SYNTHETIC', 'conidEx': '101'}])])
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.requests.request', request)
    with client.market_data_session():
        assert client.get_market_data_snapshot('101')[0]['SYMBOL'] == 'SYNTHETIC'
    assert request.call_count == 3
    assert all('fields=' in call.args[1] for call in request.call_args_list)
    assert request.call_args.kwargs['headers']['Authorization'] == 'Bearer ' + client.sessions[-1]


def test_no_automatic_market_session_recreation_outside_extract_context(client, monkeypatch):
    client.sso_token = 'existing'
    request = Mock(return_value=response({}, 401))
    monkeypatch.setattr('src.utils.connectors.ibkr_trading_api.requests.request', request)
    with pytest.raises(ServiceError):
        client.get_securities_by_symbol('SYNTHETIC', 'STK')
    assert request.call_count == 1 and not client.sessions


def test_persistent_history_session_error_fails_extract_instead_of_blank_yield(monkeypatch):
    api = Mock()
    api.get_securities_by_symbol.return_value = [{'conid': 101, 'symbol': 'SYNTHETIC'}]
    api.get_market_data_snapshot.return_value = [{'SYMBOL': 'SYNTHETIC', 'conidEx': '101', 'LAST_PRICE': '100'}]
    api.get_historical_market_data.side_effect = ServiceError('expired', 409, code='ibkr_session_expired')
    monkeypatch.setattr(etl, 'ibkr_trading_api', api)
    monkeypatch.setattr(etl.time, 'sleep', lambda _: None)
    with pytest.raises(ServiceError, match='expired'):
        etl._extract_equity_like_snapshot(['SYNTHETIC'], 'stocks_snapshot', 'STOCK', upload=False, dev_mode=True)


@pytest.mark.parametrize('annual_return', [10, 0, -5])
def test_background_history_preserves_paged_price_returns_and_coverage(monkeypatch, annual_return):
    import pandas as pd
    api = Mock()
    api.get_securities_by_symbol.return_value = [{'conid': 101, 'symbol': 'SYNTHETIC'}]
    api.get_market_data_snapshot.return_value = [{'SYMBOL': 'SYNTHETIC', 'conidEx': '101', 'LAST_PRICE': '100'}]
    bars = [{'t': pd.Timestamp(f'{2021 + offset}-10-22').value // 10**6,
             'c': 100 * (1 + annual_return / 100) ** offset} for offset in range(6)]
    api.get_historical_market_data.side_effect = [{'data': bars[2:]}, {'data': bars[:2]}]
    sleeps = []
    monkeypatch.setenv('DEV_MODE', 'false')
    monkeypatch.setattr(etl, 'ibkr_trading_api', api)
    monkeypatch.setattr(etl.time, 'sleep', sleeps.append)
    frame = etl._extract_equity_like_snapshot(['SYNTHETIC'], 'stocks_snapshot', 'STOCK',
                    config={'historical_yields': True}, upload=False)
    assert frame['Current Yield'].iloc[0] == annual_return
    assert frame.attrs['historical_yields']['populated'] == 1
    assert frame.attrs['historical_yields']['unavailable'] == 0
    assert sleeps.count(1.25) == 2
    assert api.get_historical_market_data.call_args.kwargs['direction'] == -1


def test_extractor_recovery_is_retained_in_durable_step_metadata(monkeypatch):
    import pandas as pd
    @contextmanager
    def session():
        api._market_data_context = {'recoveries': [{'reason': 'ibkr_session_expired', 'endpoint': 'secdef/search'}]}
        try:
            yield
        finally:
            api._market_data_context = None
    api = Mock()
    api.market_data_session = session
    monkeypatch.setattr(etl, 'ibkr_trading_api', api)
    @etl._with_ibkr_market_data_session
    def extract(config):
        config['_extracted_source'] = {'id': 'source', 'name': 'current.csv'}
        return pd.DataFrame([{'Last': 101}])
    config = {'name': 'stocks_snapshot', 'pipeline': 'market_data', 'extract_func': extract}
    result = etl.extract_data({'files': [config]})
    assert result['steps'][0]['session_recoveries'] == [{'reason': 'ibkr_session_expired', 'endpoint': 'secdef/search'}]
