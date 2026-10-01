from unittest.mock import patch
from pathlib import Path
import sys

import pytest
from flask import Flask

sys.path.insert(0, str(Path(__file__).parents[1]))

from src.app.tools.public.reporting import bp
from src.components.tools.public import reporting


SAMPLE = b"""Account Information,Data,Account,U6413691
Change in NAV,Data,Starting Value,1000
Change in NAV,Data,Ending Value,1206
Statement of Funds,Header,Currency,Report Date,Activity Date,Description,Debit,Credit,Balance
Statement of Funds,Data,Base Currency Summary,2026-08-03,,Opening Balance,,,1000
Statement of Funds,Data,Base Currency Summary,2026-08-04,2026-08-04,Cash Transfer,,200,1200
Statement of Funds,Data,Base Currency Summary,2026-08-05,2026-08-05,Withdrawal Fee,-2,,1198
Statement of Funds,Data,Base Currency Summary,2026-08-31,,Closing Balance,-2,200,1198
Statement of Funds,Data,USD,2026-08-04,2026-08-04,Cash Transfer,,200,1200
"""


def test_statement_summary_pairs_files_and_reconciles_cash():
    files = [
        {'id': 'csv-1', 'name': 'U6413691_202608.csv'},
        {'id': 'pdf-1', 'name': 'U6413691_202608.pdf'},
        {'id': 'pdf-2', 'name': 'U6413691_202607.pdf'},
    ]
    with patch.object(reporting, 'activity_statement_folders', {'U6413691': 'folder'}), \
         patch.object(reporting.Drive, 'get_files_in_folder', return_value=files), \
         patch.object(reporting.Drive, 'download_file', return_value=SAMPLE):
        rows = reporting.get_ibkr_statement_summary(['U6413691'], ['2026'])

    assert len(rows) == 2
    pdf_only, paired = rows
    assert pdf_only['source_file_id'] is None
    assert pdf_only['pdf_file_id'] == 'pdf-2'
    assert pdf_only['statement_complete'] is False
    assert paired['pdf_file_id'] == 'pdf-1'
    assert paired['statement_complete'] is True
    assert paired['cash_reconciled'] is True
    assert paired['credits'] == 200
    assert paired['debits'] == -2


def test_transactions_exclude_opening_closing_and_currency_duplicates():
    files = [{'id': 'csv-1', 'name': 'U6413691_202608.csv'}]
    with patch.object(reporting, 'activity_statement_folders', {'U6413691': 'folder'}), \
         patch.object(reporting.Drive, 'get_files_in_folder', return_value=files), \
         patch.object(reporting.Drive, 'download_file', return_value=SAMPLE):
        rows = reporting.get_ibkr_statement_transactions(['U6413691'], ['2026'], ['08'])

    assert len(rows) == 2
    assert [row['amount'] for row in rows] == [200, -2]


def test_mismatched_statement_account_fails():
    files = [{'id': 'csv-1', 'name': 'U6413691_202608.csv'}]
    with patch.object(reporting, 'activity_statement_folders', {'U6413691': 'folder'}), \
         patch.object(reporting.Drive, 'get_files_in_folder', return_value=files), \
         patch.object(reporting.Drive, 'download_file', return_value=SAMPLE.replace(b'Account,U6413691', b'Account,F2074691')):
        with pytest.raises(reporting.ServiceError, match='account mismatch'):
            reporting.get_ibkr_statement_summary(['U6413691'], ['2026'], ['08'])


def test_statement_of_funds_with_account_column():
    files = [{'id': 'csv-1', 'name': 'U6413691_202608.csv'}]
    with_account_column = SAMPLE.replace(
        b'Activity Date,Description,Debit', b'Activity Date,Account,Description,Debit'
    ).replace(
        b'2026-08-03,,Opening Balance', b'2026-08-03,,,Opening Balance'
    ).replace(
        b'2026-08-04,2026-08-04,Cash Transfer', b'2026-08-04,2026-08-04,,Cash Transfer'
    ).replace(
        b'2026-08-05,2026-08-05,Withdrawal Fee', b'2026-08-05,2026-08-05,,Withdrawal Fee'
    ).replace(
        b'2026-08-31,,Closing Balance', b'2026-08-31,,,Closing Balance'
    )
    with patch.object(reporting, 'activity_statement_folders', {'U6413691': 'folder'}), \
         patch.object(reporting.Drive, 'get_files_in_folder', return_value=files), \
         patch.object(reporting.Drive, 'download_file', return_value=with_account_column):
        summary = reporting.get_ibkr_statement_summary(['U6413691'], ['2026'], ['08'])
        transactions = reporting.get_ibkr_statement_transactions(['U6413691'], ['2026'], ['08'])

    assert summary[0]['cash_reconciled'] is True
    assert [row['amount'] for row in transactions] == [200, -2]


def test_statement_routes_forward_filters():
    app = Flask(__name__)
    app.register_blueprint(bp, url_prefix='/reporting')
    with patch('src.app.tools.public.reporting.get_ibkr_statement_summary', return_value=[]) as summary, \
         patch('src.app.tools.public.reporting.get_ibkr_statement_transactions', return_value=[]) as transactions:
        client = app.test_client()
        assert client.get('/reporting/ibkr_statement_summary?account=U6413691&year=2026&month=08').status_code == 200
        assert client.get('/reporting/ibkr_statement_transactions?account=U6413691&year=2026&month=08').status_code == 200

    summary.assert_called_once_with(['U6413691'], ['2026'], ['08'])
    transactions.assert_called_once_with(['U6413691'], ['2026'], ['08'])
