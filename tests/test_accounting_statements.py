"""Accounting controls exercised through parsers, preserved originals and HTTP auth."""
import base64
import copy
import io
import json
import zipfile
from datetime import date
from decimal import Decimal
from unittest.mock import Mock

import pytest
from flask import Flask, g
from flask_jwt_extended import JWTManager, create_access_token

from src.app.tools.public.reporting import bp
from src.components.tools.public import accounting_statements as service
from src.components.tools.public.accounting_parsers import (
    StatementError, money, parse_bank_summary, parse_bank_transactions, parse_ibkr, reconcile,
)
from src.utils import authz
from src.utils.exception import ServiceError


def account(institution='Banesco'):
    return dict(account='9999001111', source_account='9999001111', suffix='1111',
                institution=institution, owner='Synthetic fixture', status='open',
                opened_on='2026-01-01', closed_on=None, currency='USD',
                folder_id='fixture-folder', required_formats=['pdf', 'csv'])


IBKR = b'''Account Information,Data,Account,U-TEST
Account Information,Data,Base Currency,USD
Change in NAV,Data,Starting Value,1000
Change in NAV,Data,Ending Value,1206
Statement of Funds,Header,Currency,Report Date,Activity Date,Description,Debit,Credit,Balance,Reference,Code
Statement of Funds,Data,Base Currency Summary,2026-08-01,,Opening Balance,,,1000,,
Statement of Funds,Data,Base Currency Summary,2026-08-04,2026-08-03,Cash Transfer,,200,1200,REF1,DEP
Statement of Funds,Data,Base Currency Summary,2026-08-05,2026-08-05,Withdrawal Fee,-2,,1198,REF2,FEE
Statement of Funds,Data,Base Currency Summary,2026-08-31,,Closing Balance,-2,200,1198,,
Statement of Funds,Data,USD,2026-08-04,2026-08-03,Cash Transfer,,200,1200,REF1,DEP
'''


@pytest.fixture(autouse=True)
def isolated_cache():
    service.clear_cache()
    yield
    service.clear_cache()


def test_counterbalanced_wrong_statement_totals_never_reconcile():
    a = account('IBKR'); a.update(account='U-TEST', source_account='U-TEST')
    wrong = IBKR.replace(b'Closing Balance,-2,200,1198', b'Closing Balance,-801,999,1198')
    parsed = parse_ibkr(wrong, a, 2026, 8)
    result = reconcile(parsed, parsed['transactions'])
    assert result['differences']['statement_balance'] == 0
    assert result['differences']['transaction_balance'] == 0
    assert result['differences']['credit_total'] == -799
    assert result['differences']['debit_total'] == -799
    assert result['reconciled'] is False


def test_ibkr_preserves_activity_and_posted_dates_references_codes_and_excludes_currency_duplicates():
    a = account('IBKR'); a.update(account='U-TEST', source_account='U-TEST')
    parsed = parse_ibkr(IBKR, a, 2026, 8)
    assert len(parsed['transactions']) == 2
    assert reconcile(parsed, parsed['transactions'])['reconciled']
    tx = parsed['transactions'][0]
    assert (tx['transaction_date'], tx['posted_date'], tx['transaction_reference'], tx['transaction_code']) == ('2026-08-03', '2026-08-04', 'REF1', 'DEP')
    assert [t['amount'] for t in parsed['transactions']] == [Decimal(200), Decimal(-2)]


@pytest.mark.parametrize('replacement', [
    (b'Base Currency,USD', b'Base Currency,EUR'),
    (b'Account,U-TEST', b'Account,U-OTHER'),
    (b'Closing Balance,-2,200,1198', b'Closing Balance,,,1198'),
    (b'Cash Transfer,,200,1200', b'Cash Transfer,,NaN,1200'),
    (b'2026-08-31', b'2026-09-30'),
])
def test_ibkr_invalid_currency_identity_totals_numbers_or_period_fail(replacement):
    a = account('IBKR'); a.update(account='U-TEST', source_account='U-TEST')
    with pytest.raises(StatementError):
        parse_ibkr(IBKR.replace(*replacement), a, 2026, 8)


@pytest.mark.parametrize('value,expected', [('($1,200.05)', '-1200.05'), ('- $10.00', '-10.00'), ('+ $20', '20'), (0, '0'), ('.80', '.80')])
def test_source_money_formats(value, expected):
    assert money(value) == Decimal(expected)


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '1.2.3', ''])
def test_invalid_money_never_becomes_zero(value):
    with pytest.raises(StatementError): money(value)


@pytest.mark.parametrize('institution,text,opening,credits,debits,ending', [
    ('Banesco', 'ACCOUNT NUMBER\n9999001111\nSeptember 30, 2026\nBeginning balance on September 1, 2026 $100.00\nDeposits and other credits $50.00\nChecks and withdrawals $20.00\nService fees $1.00\nEnding balance on September 30, 2026 $129.00', '100', '50', '21', '129'),
    ('Terrabank', 'Account Number: ******1111\nStatement Date 09/30/2026\nBeginning Balance as of 09/01/2026 $100.00 Service Charges for Period $1.00\n+ Deposits and Credits (1) $50.00\n- Withdrawals and Debits (2) $21.00\nEnding Balance as of 09/30/2026 $129.00', '100', '50', '21', '129'),
    ('PNC BANK', 'Account Number: XX-XXXX-1111\nFor the Period 09/01/2026 to 09/30/2026\nBalance Summary\nBeginning\nbalance\nDeposits and\nother additions\nChecks and other\ndeductions\nEnding\nbalance\n100.00 50.00 21.00 129.00', '100', '50', '21', '129'),
    ('Truist', 'TRUIST SIMPLE BUSINESS CHECKING 9999001111\nYour previous balance as of 08/31/2026 $100.00\nChecks - 1.00\nOther withdrawals, debits and service charges - 20.00\nDeposits, credits and interest + 50.00\nYour new balance as of 09/30/2026 = $129.00', '100', '50', '21', '129'),
    ('BAC Panama', 'SEP/30/26\nRESUMEN DE CUENTA BANCARIA\n9999001111 2 21.00 1 50.00 100.00 100.00 129.00\nDETALLE DE CUENTA\n9999001111\nSaldo Anterior 100.00\nSaldo al Corte 21.00 150.00 129.00', '100', '50', '21', '129'),
    ('Mercury', 'September 2026 statement\nAccount Number 9999001111\nBeginning Balance $100.00\nTotal withdrawals $21.00\nTotal deposits $50.00\nStatement balance $129.00', '100', '50', '21', '129'),
    ('Chase', 'Account Number: 0009999001111\nSeptember 01, 2026 through September 30, 2026\nCHECKING SUMMARY\nBeginning Balance $100.00\nDeposits and Additions 1 50.00\nElectronic Withdrawals 1 -20.00\nFees 1 -1.00\nEnding Balance 3 $129.00', '100', '50', '21', '129'),
])
def test_bank_summary_adapters_verify_independent_statement_fields(institution, text, opening, credits, debits, ending):
    parsed = parse_bank_summary(text, account(institution), 2026, 9)
    assert [parsed[k] for k in ['opening', 'credits', 'debits', 'ending']] == list(map(Decimal, [opening, credits, debits, ending]))
    with pytest.raises(StatementError): parse_bank_summary(text, account(institution), 2026, 8)
    changed = account(institution); changed.update(source_account='8888002222', suffix='2222')
    with pytest.raises(StatementError): parse_bank_summary(text, changed, 2026, 9)


@pytest.mark.parametrize('institution,content', [
    ('Banesco', b'Account Number,Post Date,Check,Description,Debit,Credit,Status,Balance\n1111,9/1/2026,REF1,Fee,21,,Posted,\n1111,9/2/2026,,Deposit,,50,Posted,\n'),
    ('Terrabank', b'"date","description","amount","balance"\n"09/01/2026","Fee","- $21.00","$79.00"\n"09/02/2026","Deposit","$50.00","$129.00"\n'),
    ('PNC BANK', b'Transaction Date,Transaction Description,Amount\n2026-09-01,Fee,- $21\n2026-09-02,Deposit,+ $50\n'),
    ('Truist', b'Posted Date,Transaction Date,Transaction Type,Check/Serial #,Description,Amount,Daily Posted Balance\n09/01/2026,09/01/2026,Debit,REF1,Fee,($21),79\n09/02/2026,09/02/2026,Deposit,,Deposit,$50,129\n'),
    ('Chase', b'Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #\nDEBIT,09/01/2026,Fee,-21,FEE,79,REF1\nCREDIT,09/02/2026,Deposit,50,WIRE,129,\n'),
])
def test_bank_raw_exports_use_source_signs_without_netting_or_deduplicating(institution, content):
    tx = parse_bank_transactions(content, account(institution))
    assert [t['amount'] for t in tx] == [Decimal(-21), Decimal(50)]
    summary = dict(opening=Decimal(100), ending=Decimal(129), credits=Decimal(50), debits=Decimal(21))
    assert reconcile(summary, tx)['reconciled']
    assert not reconcile(summary, tx + [tx[0]])['reconciled']


def test_bac_xls_extracts_period_movements_instead_of_current_balance_and_summary_footer():
    import pandas as pd
    a = account('BAC Panama')
    rows = [
        ['Producto', '9999001111', '', '', '', 'USD', '', 'Saldo en Libros', '', '9999'],
        ['Fecha', 'Referencia', '', 'Código', 'Descripción', '', '', 'Débitos', 'Créditos', 'Balance*'],
        ['', '', '', '', 'Saldo Inicial', '', '', '', '', 100],
        ['01/09/2026', 123, '', 'PO', 'Fee', '', '', 21, 0, 79],
        ['02/09/2026', 456, '', 'WC', 'Deposit', '', '', 0, 50, 129],
        ['Cuadro de Resumen'], ['Código', 'Código Movimiento'], ['Totales'],
    ]
    stream = io.BytesIO(); pd.DataFrame(rows).to_excel(stream, header=False, index=False)
    tx = parse_bank_transactions(stream.getvalue(), a, 'xlsx')
    assert len(tx) == 2
    assert tx[0]['transaction_reference'] == '123'
    assert tx[0]['transaction_code'] == 'PO'
    assert [t['amount'] for t in tx] == [Decimal(-21), Decimal(50)]


def test_expected_months_include_wholly_absent_files_and_lifecycle_dates(monkeypatch):
    a = account(); a.update(opened_on='2026-02-12', closed_on='2026-04-05', status='closed')
    monkeypatch.setattr(service, 'load_registry', lambda: {'revision': 'seed', 'accounts': [a]})
    monkeypatch.setattr(service, 'Drive', Mock(get_files_in_folder=Mock(return_value=[])))
    report = service.inventory_report(2026, today=date(2026, 10, 14))
    assert [r['month'] for r in report['rows'] if r['required']] == [2, 3, 4]
    assert report['kpis']['required_months'] == 3
    assert report['kpis']['missing_or_invalid_months'] == 3
    assert report['kpis']['all_statements_backed_up'] is False


def test_unknown_closing_date_blocks_completeness_instead_of_assuming_current_month(monkeypatch):
    a = account(); a.update(status='closed', closed_on=None)
    monkeypatch.setattr(service, 'load_registry', lambda: {'revision': 'seed', 'accounts': [a]})
    monkeypatch.setattr(service, 'Drive', Mock(get_files_in_folder=Mock(return_value=[])))
    report = service.inventory_report(2026, today=date(2026, 10, 14))
    assert report['kpis']['required_months'] == 0
    assert report['kpis']['unresolved_months'] == 9
    assert all(r['required'] is False for r in report['rows'][9:])
    assert not report['kpis']['all_statements_backed_up']


def test_authorized_unknown_closing_date_policy_keeps_months_in_coverage_without_inventing_date(monkeypatch):
    a = account(); a.update(status='closed', opened_on='2026-02-12', closed_on=None, ignore_unknown_closing_date=True)
    monkeypatch.setattr(service, 'load_registry', lambda: {'revision': 'seed', 'accounts': [a]})
    monkeypatch.setattr(service, 'Drive', Mock(get_files_in_folder=Mock(return_value=[])))
    report = service.inventory_report(2026, today=date(2026, 10, 14))
    assert [r['month'] for r in report['rows'] if r['required']] == list(range(2, 10))
    assert report['kpis']['required_months'] == 8
    assert report['kpis']['unresolved_months'] == 0
    assert report['kpis']['missing_or_invalid_months'] == 8
    assert report['accounts'][0]['closed_on'] is None
    assert not report['kpis']['all_statements_backed_up']
    # If an actual closing date becomes available, it still supplies a cutoff.
    a['closed_on'] = '2026-04-05'
    assert service._expected(a, 2026, 5, date(2026, 10, 14))[0] is False


def source(key, extension='csv', monthly=True):
    return dict(id=key, key=key, name=f'1111_202609.{extension}', original=True, monthly=monthly)


def test_duplicate_sources_need_explicit_selection_and_xls_never_satisfies_csv_requirement():
    a = account(); sources = {'pdf': [source('pdf1', 'pdf'), source('pdf2', 'pdf')], 'csv': [source('csv1')], 'alternative_raw': []}
    row = service._month_row(a, 2026, 9, sources, [], date(2026, 10, 14))
    assert not row['backed_up'] and row['status'] == 'source_exception'
    a['source_selections'] = {'2026-09': {'pdf': 'pdf1'}}
    assert service._month_row(a, 2026, 9, sources, [], date(2026, 10, 14))['backed_up']
    sources['csv'] = []; sources['alternative_raw'] = [source('xls1', 'xls')]
    row = service._month_row(a, 2026, 9, sources, [], date(2026, 10, 14))
    assert row['selected_raw']['key'] == 'xls1' and not row['backed_up']


def test_multipart_exports_are_analyzed_without_claiming_monthly_backup(monkeypatch):
    a = account('Terrabank')
    f = dict(id='quarter', name='1111_202607-0930.csv', mimeType='text/csv', size='100', path='', key='quarter')
    monkeypatch.setattr(service, '_walk', lambda *_: [f])
    monkeypatch.setattr(service, 'source_bytes', lambda _: b'date,description,amount,balance\n07/01/2026,A,1,1\n08/01/2026,B,1,2\n09/01/2026,C,1,3\n')
    periods, _, _ = service.account_sources(a, 2026)
    assert all(periods[m]['csv'][0]['monthly'] is False for m in [7, 8, 9])


@pytest.mark.parametrize('content,is_csv', [
    (b'Account Number,Post Date,Check,Description,Debit,Credit,Status,Balance\n9999001111,07/01/2026,,Deposit,,10,Posted,110\n', True),
    (b'%PDF-1.7\noriginal PDF bytes', False),
    (b'not a supported transaction export', False),
])
def test_csv_with_pdf_filename_requires_verified_transaction_contents(monkeypatch, content, is_csv):
    a = account()
    original = dict(id='misnamed', key='misnamed', name='1111_202607.pdf', mimeType='text/csv', path='')
    monkeypatch.setattr(service, '_walk', lambda *_: [original])
    monkeypatch.setattr(service, 'source_bytes', lambda _: content)
    periods, _, _ = service.account_sources(a, 2026)
    selected = periods[7]['csv' if is_csv else 'pdf']
    assert len(selected) == 1
    assert selected[0]['name'] == original['name']
    assert selected[0]['source_format'] == ('csv' if is_csv else 'pdf')
    assert not periods[7]['pdf' if is_csv else 'csv']


def test_bank_unreadable_summary_cannot_be_reported_as_zero(monkeypatch):
    a = account(); sources = {'pdf': [source('pdf1', 'pdf')], 'csv': [source('csv1')], 'alternative_raw': []}
    row = service._month_row(a, 2026, 9, sources, [], date(2026, 10, 14))
    report = {'accounts': [a], 'rows': [row for _ in range(12)]}
    monkeypatch.setattr(service, 'inventory_report', lambda *_: copy.deepcopy(report))
    monkeypatch.setattr(service, '_summary', Mock(side_effect=StatementError('Unreadable summary.')))
    result = service.analyze_month(a['account'], 2026, 9)
    assert result['analysis'] is None and result['reconciled'] is False
    assert result['status'] == 'parse_error' and 'Unreadable summary.' in result['errors']


def test_continuity_difference_blocks_otherwise_correct_month(monkeypatch):
    a = account('Terrabank')
    sources = {'pdf': [source('pdf1', 'pdf')], 'csv': [source('csv1')], 'alternative_raw': []}
    row = service._month_row(a, 2026, 9, sources, [], date(2026, 10, 14))
    report = {'accounts': [a], 'rows': [row for _ in range(12)]}
    monkeypatch.setattr(service, 'inventory_report', lambda *_: copy.deepcopy(report))
    current = dict(opening=Decimal(100), ending=Decimal(129), credits=Decimal(50), debits=Decimal(21), currency='USD')
    monkeypatch.setattr(service, '_summary', Mock(side_effect=[current, dict(ending=Decimal(99))]))
    monkeypatch.setattr(service, 'source_bytes', lambda _: b'date,description,amount,balance\n09/01/2026,Fee,-21,79\n09/02/2026,Deposit,50,129\n')
    result = service.analyze_month(a['account'], 2026, 9)
    assert result['analysis']['checks']['reconciled'] is True
    assert result['continuity']['status'] == 'mismatch'
    assert result['continuity']['difference'] == 1
    assert result['reconciled'] is False


def test_annual_bundle_preserves_bytes_hashes_and_missing_month_evidence(monkeypatch):
    a = account(); original = b'%PDF-fixture-original-unchanged\x00'
    s = source('pdf1', 'pdf')
    row = service._month_row(a, 2026, 9, {'pdf': [s], 'csv': [], 'alternative_raw': []}, [], date(2026, 10, 14))
    rows = [row if m == 9 else service._month_row(a, 2026, m, {'pdf': [], 'csv': [], 'alternative_raw': []}, [], date(2026, 10, 14)) for m in range(1, 13)]
    report = {'year': 2026, 'revision': 'seed', 'accounts': [a], 'rows': rows, 'kpis': service.kpis(rows)}
    monkeypatch.setattr(service, 'inventory_report', lambda *_: report)
    monkeypatch.setattr(service, 'source_bytes', lambda _: original)
    content, filename = service.annual_bundle(2026)
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        manifest = json.loads(z.read('manifest.json'))
        entry = manifest['files'][0]
        assert z.read(entry['path']) == original
        assert entry['sha256'] == __import__('hashlib').sha256(original).hexdigest()
        assert manifest['coverage']['all_statements_backed_up'] is False
    assert filename.endswith('.zip')
    with pytest.raises(ServiceError, match='not part'):
        service.download_original(a['account'], 2026, 9, 'unrelated-file')


def test_registry_stale_edits_and_history_deletion_are_rejected_before_writes(monkeypatch):
    a = account(); b = dict(a, account='9999002222', source_account='9999002222', suffix='2222')
    monkeypatch.setattr(service, 'load_registry', lambda **_: {'revision': 'current', 'accounts': [a, b]})
    drive = Mock(); monkeypatch.setattr(service, 'Drive', drive)
    with pytest.raises(ServiceError, match='Refresh'): service.save_registry([a, b], 'stale', 'admin')
    with pytest.raises(ServiceError, match='historical'): service.save_registry([a], 'current', 'admin')
    drive.upload_file.assert_not_called()


def test_registry_concurrent_branches_fail_closed(monkeypatch):
    revisions = [{ 'revision': 'a' * 32, 'parent_revision': 'seed', 'accounts': [account()] },
                 { 'revision': 'b' * 32, 'parent_revision': 'seed', 'accounts': [account()] }]
    files = [dict(id=r['revision'], name=f"agm-accounting-registry-{r['revision']}.json") for r in revisions]
    drive = Mock(); drive.get_files_in_folder.return_value = files
    drive.download_file.side_effect = lambda key, parse: json.dumps(next(r for r in revisions if r['revision'] == key)).encode()
    monkeypatch.setattr(service, 'Drive', drive)
    with pytest.raises(ServiceError, match='conflict'): service.load_registry()


def test_upload_preserves_original_bytes_and_retains_sha_receipt(monkeypatch):
    a = account('Terrabank')
    original = b'date,description,amount,balance\n09/01/2026,Fee,-21,79\n09/02/2026,Deposit,50,129\n'
    monkeypatch.setattr(service, 'load_registry', lambda: {'accounts': [a]})
    monkeypatch.setattr(service, 'account_sources', lambda *_: ({9: {'pdf': [], 'csv': [], 'alternative_raw': []}}, [], []))
    drive = Mock(); drive.upload_file.return_value = {'id': 'created-original'}
    monkeypatch.setattr(service, 'Drive', drive)
    receipt = service.upload_original(a['account'], 2026, 9, 'bank.csv', base64.b64encode(original).decode(), 'admin')
    assert base64.b64decode(drive.upload_file.call_args_list[0].args[2]) == original
    evidence = json.loads(base64.b64decode(drive.upload_file.call_args_list[1].args[2]))
    assert evidence['sha256'] == receipt['sha256']
    assert evidence['file_id'] == 'created-original'


def test_real_http_auth_blocks_limited_users_before_any_source_access(monkeypatch):
    app = Flask(__name__); app.config['JWT_SECRET_KEY'] = 'synthetic-test-signing-key-at-least-32-bytes'
    JWTManager(app)
    users = {'limited': {'id': 'limited', 'email': 'limited@example.test', 'scopes': 'reporting'},
             'admin': {'id': 'admin', 'email': 'admin@agmtechnology.test', 'scopes': 'all'}}
    monkeypatch.setattr(authz, '_load_user', lambda identity: users.get(identity))
    app.before_request(authz.authenticate_request); app.before_request(authz.enforce_route_scope)
    app.register_blueprint(bp, url_prefix='/reporting')
    inventory = Mock(return_value={'rows': []}); monkeypatch.setattr(service, 'inventory_report', inventory)
    with app.app_context():
        limited = create_access_token(identity='limited'); admin = create_access_token(identity='admin')
    client = app.test_client()
    assert client.get('/reporting/accounting_statements?year=2026').status_code == 401
    assert client.get('/reporting/accounting_statements?year=2026', headers={'Authorization': 'Bearer ' + limited}).status_code == 403
    assert client.post('/reporting/accounting_statements/annual_download', json={'year': 2026}, headers={'Authorization': 'Bearer ' + limited}).status_code == 403
    inventory.assert_not_called()
    assert client.get('/reporting/accounting_statements?year=2026', headers={'Authorization': 'Bearer ' + admin}).status_code == 200
    assert client.get('/reporting/accounting_statements/detail?year=2026&month=9', headers={'Authorization': 'Bearer ' + admin}).status_code == 400


def test_bac_spanish_months_are_verified_from_closing_period():
    a = account('BAC Panama')
    for month, abbreviation in enumerate(['ENE','FEB','MAR','ABR','MAY','JUN','JUL','AGO','SEP','OCT','NOV','DIC'], 1):
        text = f'{abbreviation}/28/26\nRESUMEN DE CUENTA BANCARIA\n9999001111 2 21.00 1 50.00 100.00 100.00 129.00\nDETALLE DE CUENTA\n9999001111\nSaldo Anterior 100.00\nSaldo al Corte 21.00 150.00 129.00'
        assert parse_bank_summary(text, a, 2026, month)['ending'] == Decimal('129')
        with pytest.raises(StatementError): parse_bank_summary(text, a, 2026, month % 12 + 1)


def test_pdf_workers_are_safe_under_concurrent_native_reads():
    from concurrent.futures import ThreadPoolExecutor
    from reportlab.pdfgen import canvas
    stream = io.BytesIO(); pdf = canvas.Canvas(stream)
    pdf.drawString(50, 700, 'Independent PDF worker fixture'); pdf.save()
    original = stream.getvalue()
    with ThreadPoolExecutor(max_workers=4) as pool:
        texts = list(pool.map(service.pdf_text, [original] * 8))
    assert all('Independent PDF worker fixture' in text for text in texts)


def test_legacy_scanned_fee_canonicalization_retains_source_fragment_and_still_checks_independent_totals():
    text = 'Account #:******1111\nBeginning balance on January 01, 2026 $100.00\nTotal Deposits and Credits: 1 + 50.00\nTotal Checks and Debits: 1 - 20.00\nCycle Service Charge: - 1..00\nEnding balance on January 31, 2026 $129.00'
    parsed = parse_bank_summary(text, account('Terrabank'), 2026, 1)
    assert parsed['debits'] == Decimal(21)
    assert parsed['normalizations'] == [{'field': 'Cycle Service Charge', 'source_text': '1..00', 'parsed_text': '1.00'}]
    tx = parse_bank_transactions(b'date,description,amount,balance\n01/01/2026,Debit,-21,79\n01/02/2026,Deposit,50,129\n', account('Terrabank'))
    assert reconcile(parsed, tx)['reconciled']
    tx[0]['debit'] = Decimal('21.01')
    assert not reconcile(parsed, tx)['reconciled']
    with pytest.raises(StatementError): money('1..00')


def test_ibkr_separate_paxos_section_cannot_override_account_cash_or_nav():
    a = account('IBKR'); a.update(account='U-TEST', source_account='U-TEST')
    other = IBKR.replace(b'U-TEST', b'U-TEST-P (Paxos)').replace(b'Starting Value,1000', b'Starting Value,0').replace(b'Ending Value,1206', b'Ending Value,0')
    parsed = parse_ibkr(IBKR + other, a, 2026, 8)
    assert len(parsed['transactions']) == 2
    assert parsed['opening'] == 1000 and parsed['ending'] == 1198
    assert parsed['nav_ending'] == 1206


def test_ibkr_consolidated_balance_markers_and_account_column_are_supported():
    a = account('IBKR'); a.update(account='U-TEST', source_account='U-TEST')
    content = IBKR.replace(b'Activity Date,Description', b'Activity Date,Account,Description')
    content = content.replace(b',,Opening Balance,', b',,Opening Balance,,').replace(b',,Closing Balance,', b',,Closing Balance,,')
    content = content.replace(b'2026-08-03,Cash Transfer', b'2026-08-03,U-TEST,Cash Transfer').replace(b'2026-08-05,Withdrawal Fee', b'2026-08-05,U-TEST,Withdrawal Fee')
    parsed = parse_ibkr(content, a, 2026, 8)
    assert reconcile(parsed, parsed['transactions'])['reconciled']
    assert parsed['transactions'][0]['source_account'] == 'U-TEST'


def test_terrabank_ofx_csv_preserves_large_reference_and_source_type():
    tx = parse_bank_transactions(b'TRNTYPE,DTPOSTED,TRANAMT,FITID,NAME,MEMO,RUNNINGBAL\nCREDIT,01-02-2026,50.0,323666302202601020001,Name,Full memo,150\nDEBIT,01-03-2026,-21.0,323666302202601030001,Fee,Service fee,129\n', account('Terrabank'))
    assert tx[0]['transaction_date'] == '2026-01-02'
    assert tx[0]['transaction_reference'] == '323666302202601020001'
    assert tx[0]['transaction_code'] == 'CREDIT'
    assert [t['amount'] for t in tx] == [Decimal(50), Decimal(-21)]


def test_terrabank_x_masked_identity_is_still_validated_against_suffix():
    text = 'Account Number: XXXXXX1111\nStatement Date 09/30/2026\nBeginning Balance as of 09/01/2026 $100.00\n+ Deposits and Credits (1) $50.00\n- Withdrawals and Debits (2) $21.00\nEnding Balance as of 09/30/2026 $129.00'
    assert parse_bank_summary(text, account('Terrabank'), 2026, 9)['ending'] == 129
    with pytest.raises(StatementError): parse_bank_summary(text.replace('XXXXXX1111', 'XXXXXX2222'), account('Terrabank'), 2026, 9)


def test_xls_financial_reconciliation_and_required_csv_backup_are_independent(monkeypatch):
    a = account('Terrabank')
    sources = {'pdf': [source('pdf1', 'pdf')], 'csv': [], 'alternative_raw': [source('xls1', 'xls')]}
    row = service._month_row(a, 2026, 9, sources, [], date(2026, 10, 14))
    report = {'accounts': [a], 'rows': [row for _ in range(12)]}
    monkeypatch.setattr(service, 'inventory_report', lambda *_: copy.deepcopy(report))
    current = dict(opening=Decimal(100), ending=Decimal(129), credits=Decimal(50), debits=Decimal(21), currency='USD')
    monkeypatch.setattr(service, '_summary', Mock(side_effect=[current, dict(ending=Decimal(100))]))
    tx = parse_bank_transactions(b'date,description,amount,balance\n09/01/2026,Fee,-21,79\n09/02/2026,Deposit,50,129\n', a)
    monkeypatch.setattr(service, 'parse_bank_transactions', lambda *_: tx)
    monkeypatch.setattr(service, 'source_bytes', lambda _: b'fixture-original')
    result = service.analyze_month(a['account'], 2026, 9)
    assert result['reconciled'] is True
    assert result['backed_up'] is False
    assert result['transactions'][0]['account'] == a['account']
    assert result['transactions'][0]['eom'] == '2026-09-30'
    metrics = service.kpis([result])
    assert metrics['reconciled_months'] == 1 and metrics['backed_up_months'] == 0


def test_bac_opaque_record_identifier_is_not_invented_as_transaction_reference():
    row = [''] * 37
    row[0] = '1384'; row[1] = '9999001111'; row[3] = '15.04.25'; row[6] = 'PO'; row[10] = '-0.10'; row[16] = 'SHARED-EXPORT-ID'; row[18] = 'TOKEN9999001111'
    tx = parse_bank_transactions((';'.join(row) + '\n').encode(), account('BAC Panama'))
    assert tx[0]['transaction_reference'] is None
    assert tx[0]['source_export_identifier'] == 'SHARED-EXPORT-ID'
    assert tx[0]['transaction_code'] == 'PO'


def test_monthly_upload_prefix_is_not_reclassified_by_original_export_filename(monkeypatch):
    a = account('Terrabank')
    source_file = dict(id='upload', key='upload', name='9999001111_202609__export_20261014.csv', mimeType='text/csv', path='')
    monkeypatch.setattr(service, '_walk', lambda *_: [source_file])
    periods, _, _ = service.account_sources(a, 2026)
    assert len(periods[9]['csv']) == 1 and periods[9]['csv'][0]['monthly'] is True
    assert not periods[10]['csv']


def test_truist_activity_date_can_precede_statement_posting_month():
    tx = parse_bank_transactions(b'Posted Date,Transaction Date,Transaction Type,Check/Serial #,Description,Amount,Daily Posted Balance\n09/01/2026,08/31/2026,Debit,REF1,Fee,($21),79\n', account('Truist'))
    assert tx[0]['transaction_date'] == '2026-08-31'
    assert tx[0]['posted_date'] == '2026-09-01'


def test_partial_terrabank_pdf_does_not_certify_a_full_month_but_opening_month_is_allowed():
    a = account('Terrabank')
    original = 'Statement Date 09/30/2026\nAccount Number: XXXXX1111\nBeginning Balance as of 09/11/2026 $100.00\nDeposits and Credits (1) $50.00\nWithdrawals and Debits (1) $21.00\nEnding Balance $129.00'
    summary = parse_bank_summary(original, a, 2026, 9)
    assert summary['period_start'] == '2026-09-11'
    assert '2026-09-01' in summary['coverage_error']
    a['opened_on'] = '2026-09-11'
    assert parse_bank_summary(original, a, 2026, 9)['coverage_error'] is None


def test_partial_pdf_cannot_count_as_backed_up_even_when_partial_totals_reconcile(monkeypatch):
    a=account('Terrabank'); a['opened_on']='2026-09-01'
    sources={'pdf':[source('pdf','pdf')],'csv':[source('csv')],'alternative_raw':[]}
    row=service._month_row(a,2026,9,sources,[],date(2026,10,14))
    monkeypatch.setattr(service,'inventory_report',lambda *_:{'accounts':[a],'rows':[copy.deepcopy(row) for _ in range(12)]})
    summary=dict(opening=Decimal(100),ending=Decimal(129),credits=Decimal(50),debits=Decimal(21),currency='USD',coverage_error='Full-period PDF needed')
    monkeypatch.setattr(service,'_summary',lambda *_:copy.deepcopy(summary))
    raw=b'date,description,amount,balance\n09/11/2026,Fee,-21,79\n09/12/2026,Deposit,50,129\n'
    monkeypatch.setattr(service,'source_bytes',lambda _:raw)
    result=service.analyze_month(a['account'],2026,9)
    assert result['analysis']['checks']['reconciled']
    assert not result['reconciled'] and not result['backed_up']
    assert result['errors']==['Full-period PDF needed']
