import base64
import json
from unittest.mock import Mock, patch

import pytest
from src.components.tools.private.ibkr_details_refresh import refresh_details, Pacer
from src.components.tools.private import etl
from src.jobs.clients_etl import main


@pytest.fixture(autouse=True)
def no_database_startup():
    with patch('src.utils.connectors.supabase.initialize_database'):
        yield


def detail(key='U1', master='I6413690', name='old'):
    return {'account': {'accountId': key, 'masterAccountId': master},
            'associatedPersons': [{'firstName': name}]}


def client():
    c = Mock()
    c.get_bearer_token.return_value = 'synthetic-token'
    return c


def response(value=None, status=200):
    r = Mock(status_code=status)
    r.json.return_value = value
    return r


def refresh(accounts, previous, responses):
    with patch.object(Pacer, 'wait'):
        return refresh_details(accounts, previous, client_factory=client,
            get=Mock(side_effect=responses))


def test_existing_account_replaced_including_changed_person():
    values, report = refresh([{'ibkr_account_number': ' U1 ', 'master_account': 'I6413690'}],
                            [detail()], [response(detail(name='new'))])
    assert values[0]['associatedPersons'][0]['firstName'] == 'new'
    assert values[0]['_agm_refresh']['status'] == 'fresh'
    assert report['status'] == 'success'


def test_failure_retains_previous_and_marks_stale_without_success_date_guess():
    values, report = refresh([{'ibkr_account_number': 'U1', 'master_account': 'I6413690'}],
                            [detail()], [response(status=403)])
    assert values[0]['associatedPersons'][0]['firstName'] == 'old'
    assert values[0]['_agm_refresh']['status'] == 'stale'
    assert values[0]['_agm_refresh']['last_success_at'] is None
    assert report['summary']['failed'] == 1
    assert report['status'] == 'partial'


@pytest.mark.parametrize('value', [None, {}, detail(key='U2'), detail(master='F10740574')])
def test_empty_or_wrong_identity_cannot_replace_previous(value):
    values, report = refresh([{'ibkr_account_number': 'U1', 'master_account': 'I6413690'}],
                            [detail()], [response(value)])
    assert values[0]['associatedPersons'][0]['firstName'] == 'old'
    assert report['status'] == 'partial'


def test_unknown_and_conflicting_masters_never_make_requests():
    get = Mock()
    values, report = refresh_details([
        {'ibkr_account_number': 'U1', 'master_account': 'X123'},
        {'ibkr_account_number': 'U2', 'master_account': 'I6413690'},
        {'ibkr_account_number': ' U2 ', 'master_account': 'F10740574'},
        {'ibkr_account_number': 'bad'}], [], client_factory=client, get=get)
    get.assert_not_called()
    assert report['summary']['failed'] == 3
    assert values == []


def test_previous_only_account_refetched_and_auth_failure_retains_it():
    c = client()
    c.get_bearer_token.side_effect = RuntimeError('synthetic auth failure')
    values, report = refresh_details([], [detail()], client_factory=lambda: c, get=Mock())
    assert values[0]['_agm_refresh']['status'] == 'stale'
    assert report['summary']['failed'] == 1


def test_extractor_partial_is_visible_and_companion_not_transformed(tmp_path):
    fake = Mock()
    fake.get_files_in_folder.return_value = [
        {'id': 'old', 'name': 'account_details_20261013.json'},
        {'id': 'report', 'name': 'account_details_20261013.refresh.json'}]
    fake.get_most_recent_file.side_effect = lambda files: files[0]
    fake.download_file.return_value = [detail()]
    result = {'status': 'partial', 'sampled': False, 'summary': {'failed': 1}}
    with patch.object(etl, 'Drive', fake), \
         patch('src.components.clients.accounts.read_accounts', return_value=[]), \
         patch('src.components.tools.private.ibkr_details_refresh.refresh_details', return_value=([detail()], result)):
        config = {'name': 'account_details', 'backup_folder_id': 'folder',
                  'backup_name': 'account_details_20261014.json', 'extract_func': etl.extract_account_details_backup}
        overview = etl.extract_data({'files': [config]})
    assert overview['status'] == 'partial'
    assert overview['summary']['failed'] == 1
    assert len(fake.upload_file.call_args_list) == 2
    fake.delete_file.assert_not_called()
    saved = json.loads(base64.b64decode(fake.upload_file.call_args_list[0].kwargs['file_data']))
    assert saved[0]['account']['accountId'] == 'U1'


def test_runner_partial_nonzero_and_success_zero(tmp_path):
    for status, expected in [('partial', 1), ('success', 0)]:
        with patch.object(etl, 'run_pipeline', return_value={'status': status}):
            assert main(['--output-dir', str(tmp_path / status)]) == expected
        assert json.loads((tmp_path / status / 'overview.json').read_text())['status'] == status


def test_runner_local_mode_never_invokes_full_pipeline(tmp_path):
    with patch.object(etl, 'run_pipeline') as pipeline, \
         patch.object(etl, 'extract_account_details_backup', return_value={'status': 'partial'}) as extract:
        assert main(['--local-details-only', '--limit', '20', '--output-dir', str(tmp_path)]) == 1
    pipeline.assert_not_called()
    assert extract.call_args.args[0]['refresh_limit'] == 20


def test_pipeline_runs_existing_stages_with_partial_preserved():
    with patch.object(etl, 'extract_data', return_value={'status': 'partial'}) as extract, \
         patch.object(etl, 'backup_data', return_value={'status': 'success'}) as backup, \
         patch.object(etl, 'transform_data', return_value={'status': 'success'}) as transform:
        result = etl.run_pipeline('clients')
    assert result['status'] == 'partial'
    for stage in (extract, backup, transform):
        stage.assert_called_once()


def test_stable_transform_ignores_newer_companion_report():
    fake = Mock()
    fake.get_files_in_folder.side_effect = [
        [{'id': 'companion', 'name': 'account_details_20261014.refresh.json'},
         {'id': 'details', 'name': 'account_details_20261014.json'}], []]
    fake.get_most_recent_file.side_effect = lambda files: files[0]
    fake.download_file.return_value = [detail(name='fresh')]
    with patch.object(etl, 'Drive', fake):
        result = etl.process_report({'name': 'account_details', 'backup_folder_id': 'folder',
            'backup_name': 'account_details_20261014.json', 'output_filename': 'ibkr_account_details.json'})
    assert result['status'] == 'success'
    assert fake.download_file.call_args.kwargs['file_id'] == 'details'
    saved = json.loads(base64.b64decode(fake.upload_file.call_args.kwargs['file_data']))
    assert saved[0]['associatedPersons'][0]['firstName'] == 'fresh'


def test_backup_upload_failure_does_not_delete_previous():
    fake = Mock()
    fake.get_files_in_folder.return_value = []
    fake.upload_file.side_effect = RuntimeError('synthetic upload failure')
    report = {'status': 'success', 'sampled': False, 'summary': {'failed': 0}}
    with patch.object(etl, 'Drive', fake), \
         patch('src.components.clients.accounts.read_accounts', return_value=[]), \
         patch('src.components.tools.private.ibkr_details_refresh.refresh_details', return_value=([detail()], report)):
        with pytest.raises(RuntimeError, match='synthetic upload failure'):
            etl.extract_account_details_backup({'backup_folder_id': 'folder',
                'backup_name': 'account_details_20261014.json'})
    fake.delete_file.assert_not_called()


def test_local_extractor_writes_private_json_without_remote_publication(tmp_path):
    fake = Mock()
    fake.get_files_in_folder.return_value = []
    report = {'status': 'partial', 'sampled': True, 'summary': {'failed': 0}}
    with patch.object(etl, 'Drive', fake), \
         patch('src.components.clients.accounts.read_accounts', return_value=[]), \
         patch('src.components.tools.private.ibkr_details_refresh.refresh_details', return_value=([detail()], report)):
        etl.extract_account_details_backup({'backup_folder_id': 'folder',
            'backup_name': 'account_details_20261014.json', 'local_output_dir': str(tmp_path), 'refresh_limit': 20})
    fake.upload_file.assert_not_called()
    fake.delete_file.assert_not_called()
    assert json.loads((tmp_path / 'account_details.json').read_text())[0]['account']['accountId'] == 'U1'
    assert (tmp_path / 'account_details.json').stat().st_mode & 0o777 == 0o600


def test_token_refreshed_between_waves_and_workers_never_mutate_client():
    c = client()
    c.get_bearer_token.side_effect = ['first-token', 'renewed-token']
    tokens = []
    def get(url, **kwargs):
        tokens.append(kwargs['headers']['Authorization'])
        return response(detail(key=url.split('/')[-2]))
    with patch.object(Pacer, 'wait'):
        values, report = refresh_details([
            {'ibkr_account_number': 'U1', 'master_account': 'I6413690'},
            {'ibkr_account_number': 'U2', 'master_account': 'I6413690'}], [],
            workers=1, client_factory=lambda: c, get=get)
    assert tokens == ['Bearer first-token', 'Bearer renewed-token']
    c._apply_credentials.assert_called_once_with('I6413690')
    c.get_account_details.assert_not_called()
    assert report['summary']['successful'] == 2


def test_missing_internal_master_uses_prior_evidence_and_normalized_ids_deduplicate():
    accounts = [{'ibkr_account_number': ' U1 ', 'master_account': None},
                {'ibkr_account_number': 'U1', 'master_account': None}]
    get = Mock(return_value=response(detail(name='current')))
    with patch.object(Pacer, 'wait'):
        values, report = refresh_details(accounts, [detail()], client_factory=client, get=get)
    assert report['summary']['total'] == 1
    assert report['summary']['successful'] == 1
    get.assert_called_once()
    assert values[0]['associatedPersons'][0]['firstName'] == 'current'


def test_failed_refresh_preserves_known_last_success_time():
    old = detail()
    old['_agm_refresh'] = {'status': 'fresh', 'last_success_at': '2026-10-13T10:00:00+00:00'}
    values, report = refresh([], [old], [response(status=500)])
    assert values[0]['_agm_refresh']['last_success_at'] == '2026-10-13T10:00:00+00:00'
    assert old['_agm_refresh']['status'] == 'fresh'


def test_sample_cannot_publish_as_success():
    with patch.object(Pacer, 'wait'):
        values, report = refresh_details([
            {'ibkr_account_number': 'U1', 'master_account': 'I6413690'},
            {'ibkr_account_number': 'U2', 'master_account': 'I6413690'}],
            [detail(key='U2')], limit=1, client_factory=client,
            get=Mock(return_value=response(detail())))
    assert report['status'] == 'partial' and report['sampled']
    assert values[1]['_agm_refresh']['status'] == 'stale'
    assert values[1]['_agm_refresh']['error'] == 'not_selected_in_sample'
