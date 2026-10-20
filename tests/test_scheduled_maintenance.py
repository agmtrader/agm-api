"""Regressions from the October 19 runs, using in-memory provider boundaries."""
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from flask import Flask
from src.components.tools.private import etl, actions
from src.components.clients import contacts
from src.utils.response import format_response

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('scheduled_summary', ROOT / '.github/scripts/scheduled_summary.py')
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)


class DriveFixture:
    """Exercise upload, metadata verification and update ordering without Drive writes."""
    def __init__(self, old=True, failure=None):
        self.files_by_id = {'old': {'name': 'ibkr_nav_in_base.csv', 'raw': b'old report'}} if old else {}
        self.failure = failure
        self.events = []
        self.service = self

    def files(self):
        return self

    def get_files_in_folder(self, folder):
        return [{'id': key, 'name': f['name']} for key, f in self.files_by_id.items()]

    def upload_file(self, **kwargs):
        self.events.append('stage-upload')
        if self.failure == 'upload':
            raise TimeoutError('observed upload timeout')
        self.files_by_id['stage'] = {'name': kwargs['file_name'], 'raw': base64.b64decode(kwargs['file_data'])}
        return {'id': 'stage'}

    def metadata(self, key):
        file = self.files_by_id[key]
        return {'id': key, 'name': file['name'], 'size': str(len(file['raw'])),
                'md5Checksum': hashlib.md5(file['raw']).hexdigest()}

    def get(self, fileId, **kwargs):
        self.events.append('verify-stage')
        result = self.metadata(fileId)
        if self.failure == 'checksum':
            result['md5Checksum'] = 'wrong'
        return Mock(execute=lambda: result)

    def update(self, fileId, media_body=None, body=None, **kwargs):
        def execute():
            self.events.append('publish')
            if self.failure == 'update':
                raise TimeoutError('uncertain canonical update')
            if media_body:
                self.files_by_id[fileId]['raw'] = media_body.getbytes(0, media_body.size())
            if body:
                self.files_by_id[fileId]['name'] = body['name']
            return self.metadata(fileId)
        return Mock(execute=execute)

    def delete_file(self, file_id):
        self.events.append('cleanup')
        assert file_id != 'old', 'The stable report must never be deleted'
        del self.files_by_id[file_id]


@pytest.mark.parametrize('failure', ['upload', 'checksum', 'update'])
def test_nav_failure_preserves_stable_report_and_original_error(failure):
    drive = DriveFixture(failure=failure)
    with patch.object(etl, 'Drive', drive), pytest.raises((TimeoutError, ValueError)):
        etl._publish_report('ibkr_nav_in_base.csv', 'text/csv', [{'Total': 123}])
    assert drive.files_by_id['old']['raw'] == b'old report'
    assert 'cleanup' not in drive.events
    from src.components.tools.public import reporting
    drive.download_file = Mock(return_value=[{'Total': 'old'}])
    with patch.object(reporting, 'Drive', drive):
        assert reporting._read_daily_report('ibkr_nav') == [{'Total': 'old'}]
    drive.download_file.assert_called_once_with(file_id='old', parse=True)
    if failure == 'checksum':
        assert 'publish' not in drive.events


@pytest.mark.parametrize('old', [True, False])
def test_verified_publication_updates_same_id_or_promotes_stage(old):
    drive = DriveFixture(old=old)
    with patch.object(etl, 'Drive', drive):
        result = etl._publish_report('ibkr_nav_in_base.csv', 'text/csv', [{'Total': 123}])
    assert drive.events[:3] == ['stage-upload', 'verify-stage', 'publish']
    assert result['id'] == ('old' if old else 'stage')
    assert drive.files_by_id[result['id']]['raw'] == b'Total\n123\n'
    assert len(drive.files_by_id) == 1


def test_market_dataframe_result_is_metadata_and_transform_uses_exact_source():
    drive = Mock()
    drive.upload_file.return_value = {'id': 'fresh-source'}
    def extract(config):
        frame = pd.DataFrame([{'Last': 123}])
        etl._upload_market_snapshot(config, frame)
        return frame
    config = {'name': 'stocks_snapshot', 'pipeline': 'market_data', 'extract_func': extract,
              'backup_name': 'stock_current.csv', 'backup_folder_id': 'backup', 'output_filename': 'output.csv'}
    drive.get_files_in_folder.return_value = [{'id': 'other', 'name': 'stock_newer.csv'},
                                              {'id': 'fresh-source', 'name': 'stock_current.csv'}]
    drive.get_most_recent_file.side_effect = lambda files: files[0]
    drive.download_file.return_value = [{'Last': 123}]
    with patch.object(etl, 'Drive', drive), patch.object(etl, '_publish_report') as publish:
        result = etl.extract_data({'files': [config]})
        transformed = etl.process_report(config)
    assert result['status'] == 'success'
    assert result['steps'][0]['row_count'] == 1
    assert transformed['source_file_id'] == 'fresh-source'
    drive.download_file.assert_called_once_with(file_id='fresh-source', parse=True)
    publish.assert_called_once()


def test_failed_market_extract_cannot_publish_previous_backup():
    drive = Mock()
    drive.get_files_in_folder.return_value = [{'id': 'old', 'name': 'stock_old.csv'}]
    config = {'name': 'stocks_snapshot', 'pipeline': 'market_data', 'backup_folder_id': 'backup',
              'output_filename': 'output.csv', 'extract_func': Mock(side_effect=TimeoutError('provider failed'))}
    with patch.object(etl, 'Drive', drive), patch.object(etl, '_publish_report') as publish:
        assert etl.extract_data({'files': [config]})['status'] == 'partial'
        with pytest.raises(ValueError, match='No source from this market ETL run'):
            etl.process_report(config)
    publish.assert_not_called()
    drive.delete_file.assert_not_called()


def test_report_preserves_exact_upload_exception():
    drive = Mock()
    drive.get_files_in_folder.return_value = [{'id': 'source', 'name': 'nav.csv'}]
    drive.get_most_recent_file.side_effect = lambda files: files[0]
    drive.download_file.return_value = [{'Total': 100}]
    original = TimeoutError('synthetic Drive timeout')
    with patch.object(etl, 'Drive', drive), patch.object(etl, '_publish_report', side_effect=original):
        with pytest.raises(TimeoutError) as caught:
            etl.process_report({'name': 'nav', 'backup_folder_id': 'folder', 'output_filename': 'nav.csv'})
    assert caught.value is original
    drive.delete_file.assert_not_called()


def screening_fixture(monkeypatch, fail=False):
    database = Mock()
    rows = {
        'account_contact': [{'account_id': 'a', 'contact_id': f'c{i}'} for i in range(3)],
        'contact_screening': [],
        'contact': [{'id': f'c{i}', 'name': f'Holder {i}'} for i in range(3)],
        'account': [{'id': 'a', 'ibkr_account_number': 'synthetic'}],
    }
    database.read.side_effect = lambda table, query=None: rows[table]
    database.create_many.side_effect = lambda **kwargs: len(kwargs['data'])
    monkeypatch.setattr(actions, 'db', database)
    monkeypatch.setattr(contacts, 'db', database)
    monkeypatch.setattr(contacts, '_get_ibkr_details_by_account_id', lambda: {})
    monkeypatch.setattr(contacts, '_compute_weighted_holder_risk_score', lambda **kwargs: 1)
    monkeypatch.setattr(contacts, '_get_contact_risk_country', lambda *args: 'US')
    monkeypatch.setattr(contacts, '_get_fatf_status', lambda country: None)
    lists = ([{'Name': 'Holder 0'}], [], [])
    # Match lookup indexes reflect the real payload matcher; risk/provider inputs are local.
    monkeypatch.setattr(contacts, '_build_sanctions_match_indexes', lambda _: ({'holder 0': [{'name': 'Holder 0'}]}, {}, {}))
    monkeypatch.setattr(actions, 'get_ofac_sdn_list', lambda: lists[0])
    monkeypatch.setattr(actions, 'get_uk_sanctions_list', lambda: [])
    monkeypatch.setattr(actions, 'get_un_sanctions_list', lambda: [])
    monkeypatch.setattr(actions, 'compare_all_sanctions_today_vs_yesterday', lambda: {
        'all_available': True, 'all_same': False,
        'ofac': {'both_available': True, 'change_summary': {'added_count': 5, 'removed_count': 0}},
    })
    if fail:
        del rows['contact'][2]
    app = Flask(__name__)
    app.add_url_rule('/actions/run_screening_process', 'screen', format_response(actions.run_screenings))
    return app.test_client(), database


@pytest.mark.parametrize('fail', [False, True])
def test_screening_flask_results_match_persisted_payloads_and_summary_gate(monkeypatch, fail):
    client, database = screening_fixture(monkeypatch, fail)
    response = client.get('/actions/run_screening_process')
    assert response.status_code == 200
    body = response.json
    assert body['total_screenings_planned'] == body['contacts_targeted'] == 3
    assert body['screenings_executed'] == (2 if fail else 3)
    payloads = database.create_many.call_args.kwargs['data']
    matched = sum(bool(p['ofac_results'] or p['uk_status'] or p['un_status']) for p in payloads)
    assert body['screenings_with_list_matches'] == matched == 1
    assert body['screenings_without_list_matches'] == len(payloads) - 1
    assert body['ofac_matches'] == 1
    assert summary.render('screenings', body, '200')[0] is (not fail)


def test_legacy_missing_screening_metrics_are_unavailable_not_zero():
    good, rendered = summary.render('screenings', {'screenings_executed': 859, 'contacts_targeted': 859,
        'screenings_skipped': False, 'screening_errors': []}, '200')
    assert not good
    assert 'screenings_with_list_matches: unavailable' in rendered
    assert 'total_screenings_planned: unavailable' in rendered
