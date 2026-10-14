"""Protect order grouping, source arithmetic, ownership lookup and HTTP access."""
from unittest.mock import Mock

import pytest
from flask import Flask
from flask_jwt_extended import JWTManager, create_access_token

from src.components.tools.public import trade_confirmations as trades
from src.app.tools.public import reporting as routes
from src.utils import authz
from src.utils.exception import ServiceError

BASE = dict(TradeDate='20261014', OrderID='ORDER-1', IsAPIOrder='N', AssetClass='STK', Symbol='TEST',
            ClientAccountID='TEST-1', AccountAlias='Synthetic fixture', OrderType='LMT',
            CurrencyPrimary='USD', CommissionCurrency='USD', LevelOfDetail='EXECUTION', Commission='-2.25', Amount='100')


def test_counterbalanced_fills_preserve_signed_commission_and_each_absolute_amount():
    report = trades.group_trade_confirmations([BASE, {**BASE, 'Commission': '0.50', 'Amount': '-100'}, {'LevelOfDetail': 'SUMMARY'}])
    assert report['execution_count'] == 2
    assert report['rows'][0]['Commission'] == -1.75
    assert report['rows'][0]['Amount'] == 200
    assert report['rows'][0]['ExecutionCount'] == 2
    assert report['trade_dates'] == ['2026-10-14']


def test_currency_date_and_account_boundaries_never_merge():
    variants = [BASE, {**BASE, 'TradeDate': '2026-10-13'}, {**BASE, 'ClientAccountID': 'TEST-2'},
                {**BASE, 'CurrencyPrimary': 'HKD'}, {**BASE, 'CommissionCurrency': 'MXN'}]
    result = trades.group_trade_confirmations(variants)
    assert len(result['rows']) == 5
    assert result['account_count'] == 2
    assert result['trade_dates'] == ['2026-10-13', '2026-10-14']


def test_query_advisor_precedes_current_mapping_and_alias_never_supplies_missing_advisor():
    result = trades.group_trade_confirmations([BASE, {**BASE, 'OrderID': 'ORDER-2', 'Advisor': 'Query advisor'},
                                             {**BASE, 'ClientAccountID': 'UNMAPPED', 'AccountAlias': 'Advisor-looking alias'}], {'TEST-1': 'Current advisor'})
    by_account = {r['ClientAccountID']: r for r in result['rows'] if r['OrderID']=='ORDER-1'}
    assert by_account['TEST-1']['Advisor'] == 'Current advisor'
    assert by_account['TEST-1']['AdvisorSource'] == 'AGM current assignment'
    assert by_account['UNMAPPED']['Advisor'] == ''
    assert result['unassigned_accounts'] == ['UNMAPPED']
    assert next(r for r in result['rows'] if r['OrderID']=='ORDER-2')['Advisor'] == 'Query advisor'


@pytest.mark.parametrize('change', [{'Commission':'NaN'}, {'Amount':'Infinity'}, {'Amount':''}, {'TradeDate':'2026-99-01'}, {'ClientAccountID':''}, {'CommissionCurrency':None}])
def test_invalid_source_fails_without_zero_totals(change):
    with pytest.raises(ServiceError): trades.group_trade_confirmations([{**BASE, **change}])


def test_empty_source_has_explicit_empty_coverage():
    result = trades.group_trade_confirmations([])
    assert result['rows'] == [] and result['account_count'] == 0 and result['unassigned_accounts'] == []
    assert result['retrieved_at']


def test_fixed_query_and_single_bounded_advisor_lookup(monkeypatch):
    read = Mock(return_value=[BASE, {**BASE, 'OrderID':'ORDER-2'}, {**BASE, 'ClientAccountID':'TEST-2', 'Advisor':'Source advisor'}])
    mapping = Mock(return_value={'TEST-1':'Assigned advisor'})
    monkeypatch.setattr(trades, 'getFlexQuery', read); monkeypatch.setattr(trades, 'read_advisor_mapping', mapping)
    result = trades.get_trade_confirmations_report()
    read.assert_called_once_with('1215183'); mapping.assert_called_once_with({'TEST-1'})
    assert not result['unassigned_accounts']


def test_mapping_failure_is_visible_not_replaced_by_unassigned(monkeypatch):
    monkeypatch.setattr(trades, 'getFlexQuery', lambda _: [BASE])
    monkeypatch.setattr(trades, 'read_advisor_mapping', Mock(side_effect=ServiceError('Assignment lookup failed',502)))
    with pytest.raises(ServiceError, match='lookup failed'): trades.get_trade_confirmations_report()


def test_http_requires_admin_before_source_read_and_ignores_query_override(monkeypatch):
    app = Flask(__name__); app.config['JWT_SECRET_KEY']='synthetic-test-signing-key-at-least-32-bytes'; JWTManager(app)
    users={'limited':{'id':'limited','email':'limited@example.test','scopes':'reporting'},'admin':{'id':'admin','email':'admin@example.test','scopes':'all'}}
    monkeypatch.setattr(authz,'_load_user',lambda identity:users.get(identity))
    app.before_request(authz.authenticate_request); app.before_request(authz.enforce_route_scope)
    app.register_blueprint(routes.bp,url_prefix='/reporting')
    query=Mock(return_value=[]); monkeypatch.setattr(trades,'getFlexQuery',query)
    with app.app_context(): limited=create_access_token(identity='limited'); admin=create_access_token(identity='admin')
    client=app.test_client()
    assert client.get('/reporting/trade_confirmations').status_code==401
    assert client.get('/reporting/trade_confirmations',headers={'Authorization':'Bearer '+limited}).status_code==403
    query.assert_not_called()
    result=client.get('/reporting/trade_confirmations?query_id=other',headers={'Authorization':'Bearer '+admin})
    assert result.status_code==200 and result.json['rows']==[]
    query.assert_called_once_with('1215183')
