"""Synthetic broker data; actual ACOBO generator and authenticated Flask route."""
import sys
from pathlib import Path
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from trade_ticket_acceptance import boundary  # noqa: F401; reusable JWT/connector boundary
import pytest
from src.components.tools.public.trade_tickets import generate_trade_confirmation_message


def mex(**changes):
    row = dict.fromkeys([
        'ClientAccountID', 'AccountAlias', 'CurrencyPrimary', 'AssetClass', 'Symbol',
        'Description', 'Conid', 'SecurityID', 'SecurityIDType', 'CUSIP', 'ISIN',
        'FIGI', 'Issuer', 'Maturity', 'Buy/Sell', 'SettleDate', 'TradeDate', 'Exchange',
    ], '')
    row.update(
        ClientAccountID='synthetic-account', CurrencyPrimary='GBP', AssetClass='BOND',
        Description='MEX 5 5/8 03/19/2114', ISIN='XS1046593908',
        SecurityID='XS1046593908', SecurityIDType='ISIN', Quantity='100000',
        Price='69.4193', AccruedInterest='3297.95', NetCash='-69437.67',
        SettleDate='2026-10-19', TradeDate='2026-10-15',
    )
    return {**row, **changes}


def fields(rows):
    message = generate_trade_confirmation_message(rows, list(range(len(rows))))['data']
    return dict(line.split(': ', 1) for line in message.splitlines() if ': ' in line)


@pytest.mark.parametrize('count', [1, 3])
def test_mex_days_and_broker_money_preserved(count):
    output = fields([mex() for _ in range(count)])
    assert output['Accrued (Days)'] == '214'
    assert float(output['Quantity']) == 100000 * count
    assert float(output['AccruedInterest']) == pytest.approx(3297.95 * count)
    assert float(output['Amount']) == pytest.approx(69437.67 * count)
    assert float(output['TotalAmount']) == pytest.approx(72735.62 * count)


@pytest.mark.parametrize('settlement,days', [
    ('2026-03-19', 0), ('2026-03-20', 1), ('2027-03-18', 364),
    ('2027-03-19', 0), ('2028-03-18', 365), ('2028-03-19', 0),
])
def test_actual_coupon_boundaries_and_leap_year(settlement, days):
    assert fields([mex(SettleDate=settlement, AccruedInterest=0)])['Accrued (Days)'] == str(days)


def test_security_id_isin_fallback_and_calendar_days_ignore_money_rounding():
    output = fields([mex(ISIN='', AccruedInterest='3297.94')])
    assert output['Accrued (Days)'] == '214'
    assert output['AccruedInterest'] == '3297.94'


@pytest.mark.parametrize('count', [1, 3])
def test_unregistered_bond_retains_legacy_360_calculation(count):
    output = fields([mex(ISIN='TEST-UNKNOWN', SecurityID='TEST-UNKNOWN',
                         Description='SYN 5 03/19/2030', AccruedInterest='500')
                     for _ in range(count)])
    assert float(output['Accrued (Days)']) == 36


def test_authenticated_dashboard_generate_response(boundary):
    http, headers, lookup, reads, email = boundary
    rows = [mex(Price=str(price)) for price in [69.4193, 69.4149, 69.4727]]
    response = http.post('/trade_tickets/generate', headers=headers(), json={
        'flex_query_dict': rows, 'indices': '0,1,2',
    })
    assert response.status_code == 200
    assert 'Accrued (Days): 214\n' in response.json['data']
    assert 'Accrued (Days): 211' not in response.json['data']
    reads.assert_not_called()
    email.assert_not_called()


@pytest.mark.parametrize('changes', [
    {'SettleDate': '2026-10-20'}, {'SettleDate': ''}, {'SettleDate': 'invalid'},
    {'SettleDate': '2114-03-19'}, {'SettleDate': '2014-03-18'},
    {'ISIN': 'OTHER-ISIN'}, {'ISIN': '', 'SecurityID': ''},
    {'ISIN': 'OTHER-ISIN', 'SecurityID': 'OTHER-ISIN'},
])
def test_incompatible_consolidation_returns_actionable_400(boundary, changes):
    http, headers, lookup, reads, email = boundary
    response = http.post('/trade_tickets/generate', headers=headers(), json={
        'flex_query_dict': [mex(), mex(**changes)], 'indices': '0,1',
    })
    assert response.status_code == 400
    assert response.json['code'] == 'invalid_bond_accrual'
    assert response.json['error']
    reads.assert_not_called()
    email.assert_not_called()


if __name__ == '__main__':
    # Optional synthetic HTTP result for a separate browser UI acceptance check.
    if '--message-json' in sys.argv:
        result = generate_trade_confirmation_message([mex() for _ in range(3)], [0, 1, 2])
        print(json.dumps(result))
    else:
        raise SystemExit(pytest.main([__file__, '-q']))
