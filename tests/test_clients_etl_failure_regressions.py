"""Regressions for the observed October 22 publication and delinked responses."""
import base64
import hashlib
import importlib.util
from io import BytesIO
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.components.tools.private import etl
from src.components.tools.private.ibkr_details_refresh import Pacer, refresh_details
from src.jobs.clients_etl import evaluate
from src.utils.exception import ServiceError


FULL = 'ibkr_open_positions_all.csv'
TEMPLATE = 'ibkr_open_positions_template.csv'


class PositionsDrive:
    """In-memory Drive transport; run the actual transform and publication code."""
    def __init__(self, rows, failure=None):
        self.rows = rows
        self.failure = failure
        self.data = {FULL: b'previous full report', TEMPLATE: b'previous template'}
        self.names = {FULL: FULL, TEMPLATE: TEMPLATE}
        self.events = []
        self.service = self

    def files(self):
        return self

    def get_files_in_folder(self, folder):
        if folder == 'positions-backup':
            return [{'id': 'source', 'name': '742588_20261021.csv'}]
        return [{'id': key, 'name': value} for key, value in self.names.items()]

    def get_most_recent_file(self, files):
        return files[0]

    def download_file(self, **kwargs):
        assert kwargs == {'file_id': 'source', 'parse': True}
        return self.rows

    def get_file_info(self, **kwargs):
        return {'id': kwargs['file_name'], 'name': kwargs['file_name']}

    def upload_file(self, **kwargs):
        self.events.append('stage')
        if self.failure == 'upload':
            raise TimeoutError('stage upload failed')
        key = kwargs['file_name']
        self.data[key] = base64.b64decode(kwargs['file_data'])
        self.names[key] = key
        return {'id': key}

    def metadata(self, key):
        return {'id': key, 'name': self.names[key], 'size': str(len(self.data[key])),
                'md5Checksum': hashlib.md5(self.data[key]).hexdigest()}

    def get(self, fileId, **kwargs):
        metadata = self.metadata(fileId)
        if self.failure == 'checksum':
            metadata['md5Checksum'] = 'wrong'
        return Mock(execute=lambda: metadata)

    def update(self, fileId, media_body=None, body=None, **kwargs):
        def execute():
            self.events.append(('update', fileId))
            if self.failure == 'update' or (self.failure == 'template' and fileId == TEMPLATE):
                raise TimeoutError('uncertain update')
            self.data[fileId] = media_body.getbytes(0, media_body.size())
            return self.metadata(fileId)
        return Mock(execute=execute)

    def delete_file(self, file_id):
        if file_id in (FULL, TEMPLATE):
            # Exact boundary and exception observed in the failed execution.
            raise ServiceError('Internal server error')
        self.events.append('cleanup')
        del self.data[file_id]
        del self.names[file_id]


def positions():
    # Column order comes from the actual 742588 source. Keep this fixture free
    # of client data; vary the observed asset/detail combinations explicitly.
    columns = ('ClientAccountID,AccountAlias,Model,CurrencyPrimary,FXRateToBase,AssetClass,Symbol,'
               'Description,Conid,SecurityID,SecurityIDType,CUSIP,ISIN,ListingExchange,UnderlyingConid,'
               'UnderlyingSymbol,UnderlyingSecurityID,UnderlyingListingExchange,Issuer,Multiplier,'
               'Strike,Expiry,Put/Call,PrincipalAdjustFactor,ReportDate,Quantity,MarkPrice,PositionValue,'
               'PositionValueInBase,OpenPrice,CostBasisPrice,CostBasisMoney,PercentOfNAV,FifoPnlUnrealized,'
               'UnrealizedCapitalGainsPnl,UnrealizedFxPnl,Side,LevelOfDetail,OpenDateTime,HoldingPeriodDateTime,'
               'VestingDate,Code,OriginatingOrderID,OriginatingTransactionID,AccruedInterest,SerialNumber,'
               'DeliveryType,CommodityType,Fineness,Weight,SubCategory,FIGI,IssuerCountryCode').split(',')
    rows = []
    for asset, level, conid in [('BOND', 'LOT', 101), ('BOND', 'SUMMARY', 101), ('STK', 'LOT', 202)]:
        row = dict.fromkeys(columns, '')
        row.update(ClientAccountID='U1', AssetClass=asset, LevelOfDetail=level,
                   Conid=conid, Quantity=10, ReportDate=20261021)
        rows.append(row)
    return rows


def run_positions(drive):
    with patch.object(etl, 'Drive', drive), patch.object(etl, 'get_bond_report',
            return_value=[{'Symbol': 'IBCID101', 'Last': 99}]):
        return etl.transform_data({'files': [{
            'name': '742588', 'pipeline': 'clients', 'backup_folder_id': 'positions-backup',
            'backup_name': '742588_20261021.csv', 'output_filename': TEMPLATE,
            'transform_func': etl.process_open_positions_template}]})


def test_positions_publish_both_reports_without_deleting_stable_files():
    rows = positions()
    drive = PositionsDrive(rows)
    result = run_positions(drive)
    assert result['status'] == 'success'
    pd.testing.assert_frame_equal(pd.read_csv(BytesIO(drive.data[FULL])).fillna(''),
                                  pd.DataFrame(rows), check_dtype=False)
    template = pd.read_csv(BytesIO(drive.data[TEMPLATE]))
    assert len(template) == 1
    assert template.iloc[0]['AssetClass'] == 'BOND'
    assert template.iloc[0]['LevelOfDetail'] == 'LOT'
    assert template.iloc[0]['Quantity'] == 10
    assert drive.names == {FULL: FULL, TEMPLATE: TEMPLATE}


@pytest.mark.parametrize('failure', ['upload', 'checksum', 'update', 'template'])
def test_positions_publication_failure_is_visible_and_preserves_stable_file(failure):
    drive = PositionsDrive(positions(), failure)
    result = run_positions(drive)
    assert result['status'] == 'partial'
    assert result['reports'][0]['status'] == 'failed'
    assert drive.data[TEMPLATE] == b'previous template'
    if failure != 'template':
        assert drive.data[FULL] == b'previous full report'
    else:
        assert drive.data[FULL] != b'previous full report'
    if failure != 'template':
        assert 'cleanup' not in drive.events


def refresh_response(account, previous=True):
    client = Mock()
    client.get_bearer_token.return_value = 'synthetic-token'
    old = {'account': {'accountId': 'U1', 'masterAccountId': 'F10740574'},
           'associatedPersons': [{'id': 'historical-person'}],
           '_agm_refresh': {'status': 'fresh', 'last_success_at': '2026-10-16T14:43:44+00:00'}}
    with patch.object(Pacer, 'wait'):
        return refresh_details([{'ibkr_account_number': 'U1', 'master_account': 'F10740574'}],
            [old] if previous else [], client_factory=lambda: client,
            get=Mock(return_value=Mock(status_code=200, json=lambda: {'account': account})))


def test_observed_delinked_shape_retains_history_without_counting_it_fresh():
    values, report = refresh_response({'accountId': 'U1', 'accountTitle': 'Synthetic holder',
                                        'dateDelinked': '20261016'})
    outcome = report['outcomes'][0]
    assert outcome['status'] == 'skipped'
    assert outcome['reason'] == 'account_delinked'
    assert outcome['date_delinked'] == '20261016'
    assert report['summary'] == {'total': 1, 'successful': 0, 'failed': 0,
                                 'skipped': 1, 'delinked': 1, 'retained_stale': 1}
    assert values[0]['associatedPersons'] == [{'id': 'historical-person'}]
    assert values[0]['_agm_refresh']['status'] == 'stale'
    assert values[0]['_agm_refresh']['error'] == 'account_delinked'
    assert values[0]['_agm_refresh']['last_success_at'] == '2026-10-16T14:43:44+00:00'


@pytest.mark.parametrize('account', [
    {'accountId': 'U2', 'dateDelinked': '20261016'},
    {'accountId': 'U1'},
    {'accountId': 'U1', 'dateDelinked': ''},
    {'accountId': 'U1', 'dateDelinked': '20260230'},
    {'accountId': 'U1', 'dateDelinked': '29990101'},
    {'accountId': 'U1', 'dateDelinked': '20261016', 'masterAccountId': 'I6413690'},
])
def test_unexplained_missing_master_wrong_identity_or_conflict_still_fails(account):
    values, report = refresh_response(account)
    assert report['summary']['failed'] == 1
    assert report['outcomes'][0]['status'] == 'failed'
    assert values[0]['_agm_refresh']['status'] == 'stale'


def test_delinked_without_previous_details_does_not_invent_a_fresh_record():
    values, report = refresh_response({'accountId': 'U1', 'dateDelinked': '20261016'}, previous=False)
    assert values == []
    assert report['outcomes'][0]['status'] == 'skipped'
    assert report['outcomes'][0]['retained_previous'] is False


def test_terminal_policy_accepts_delinking_but_no_arbitrary_skip_or_new_failure():
    result = {'status': 'partial', 'overview': {
        'backup': {'status': 'success'}, 'transform': {'status': 'success'},
        'extract': {'steps': [{'name': 'account_details', 'status': 'partial',
            'summary': {'successful': 616}, 'outcomes': [
                {'account_id': 'U1', 'status': 'skipped', 'reason': 'account_delinked', 'date_delinked': '20261016'},
                {'account_id': 'U2', 'status': 'failed', 'error': 'HTTP 403'}]}]}}}
    baseline = [{'account_id': 'U2', 'error': 'HTTP 403'}]
    assert evaluate(result, baseline) == 'completed_with_accepted_coverage_gaps'
    result['overview']['extract']['steps'][0]['outcomes'][0]['reason'] = 'unexplained'
    assert evaluate(result, baseline) == 'failed'
    result['overview']['extract']['steps'][0]['outcomes'][0].update(status='failed', error='details_master_mismatch')
    assert evaluate(result, baseline) == 'failed'


def test_summary_reports_delinked_history_without_exporting_account_identifiers(monkeypatch):
    path = Path(__file__).resolve().parents[1] / '.github/scripts/clients_etl_summary.py'
    spec = importlib.util.spec_from_file_location('clients_summary_regression', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setenv('GITHUB_SERVER_URL', 'https://github.com')
    monkeypatch.setenv('GITHUB_REPOSITORY', 'example/repo')
    monkeypatch.setenv('GITHUB_RUN_ID', '1')
    payload = {'status': 'partial', 'overview': {'extract': {'steps': [{
        'name': 'account_details', 'summary': {'successful': 1, 'failed': 0, 'retained_stale': 1},
        'outcomes': [{'account_id': 'U12345678', 'status': 'skipped', 'reason': 'account_delinked'}]}]}}}
    rendered = module.render(payload, {})
    assert '1 fresh, 0 failed, 1 retained stale' in rendered
    assert 'IBKR delinked: 1 skipped after broker confirmation' in rendered
    assert 'U12345678' not in rendered
