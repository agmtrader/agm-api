"""Sanitized fixtures follow observed contact/link/IBKR associatedPersons shapes."""
import copy
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / 'contact_duplicates.py'
spec = importlib.util.spec_from_file_location('contact_duplicates', SCRIPT)
dedup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dedup)


def fixture():
    return ({'captured_at': datetime.now(timezone.utc).isoformat(),
             'contacts': [{'id': 'c1', 'type': 'individual', 'name': 'Ana Maria López', 'email': 'a@example.org'},
                          {'id': 'c2', 'type': 'individual', 'name': 'Ana Maria Lopez', 'email': 'b@example.org'}],
             'accounts': [{'id': 'a1', 'ibkr_account_number': 'U100', 'master_account': 'F100'}],
             'account_contacts': [{'id': 'l1', 'account_id': 'a1', 'contact_id': 'c1', 'entity_id': 101},
                                  {'id': 'l2', 'account_id': 'a1', 'contact_id': 'c2', 'entity_id': 101}]},
            [{'account': {'accountId': 'U100', 'masterAccountId': 'F100'},
              '_agm_refresh': {'status': 'fresh', 'last_success_at': datetime.now(timezone.utc).isoformat()},
              'associatedPersons': [{'entityId': 101, 'firstName': 'Ana', 'middleName': 'Maria', 'lastName': 'Lopez',
                                     'dateOfBirth': '1980-01-02', 'associations': ['Account Holder'],
                                     'identityDocuments': [{'name': 'NationalID', 'country': 'CRI', 'id': '001-234-567'}]}]}])


def test_missing_local_identity_is_corroborated_by_exact_account_link():
    db, ibkr = fixture()
    report = dedup.build_report(db, ibkr)
    pair = report['candidates'][0]
    assert pair['classification'] == 'strong_candidate'
    assert {'same_account_scoped_ibkr_person', 'same_ibkr_birthday', 'same_government_id_type_country'} <= set(pair['evidence'])
    assert '001234567' == dedup.identifier('001-234-567')


def test_joint_holders_with_similar_names_are_conflicts_not_duplicates():
    db, ibkr = fixture()
    second = copy.deepcopy(ibkr[0]['associatedPersons'][0])
    second.update(entityId=202, dateOfBirth='1985-02-03')
    second['identityDocuments'][0]['id'] = '002-345-678'
    ibkr[0]['associatedPersons'].append(second)
    db['account_contacts'][1]['entity_id'] = 202
    pair = dedup.build_report(db, ibkr)['candidates'][0]
    assert pair['classification'] == 'conflicting_evidence'
    assert 'different_same_account_entity_ids' in pair['conflicts']
    assert 'different_ibkr_birthdays' in pair['conflicts']
    assert 'same_government_id_type_country' not in pair['evidence']


def test_name_fallback_does_not_circularly_supply_identity_evidence():
    db, ibkr = fixture()
    for link in db['account_contacts']:
        link.pop('entity_id')
    pair = dedup.build_report(db, ibkr)['candidates'][0]
    assert pair['classification'] == 'manual_review'
    assert not {'same_ibkr_birthday', 'same_government_id_type_country'} & set(pair['evidence'])


def test_both_populated_link_identifiers_must_match_the_same_person():
    db, ibkr = fixture()
    db['account_contacts'][1]['external_id'] = 'different-person'
    ibkr[0]['associatedPersons'][0]['externalCode'] = 'first-person'
    report = dedup.build_report(db, ibkr)
    assert report['coverage']['unresolved_holder_links'] == 1
    assert 'unresolved_link_identifiers' in report['candidates'][0]['conflicts']


def test_master_mismatch_does_not_join_same_account_number():
    db, ibkr = fixture()
    ibkr[0]['account']['masterAccountId'] = 'OTHER'
    report = dedup.build_report(db, ibkr)
    assert report['coverage']['links_missing_details'] == 2
    assert report['candidates'][0]['classification'] == 'manual_review'


def test_duplicate_backup_rows_are_reported_as_ambiguous():
    db, ibkr = fixture()
    report = dedup.build_report(db, ibkr + ibkr)
    assert report['coverage']['links_ambiguous_details'] == 2
    assert report['coverage']['duplicate_ibkr_account_keys'] == 1
    assert report['candidates'][0]['classification'] == 'manual_review'


def test_backup_without_master_is_excluded_and_counted():
    db, ibkr = fixture()
    del ibkr[0]['account']['masterAccountId']
    report = dedup.build_report(db, ibkr)
    assert report['coverage']['ibkr_details_missing_account_identity'] == 1
    assert report['coverage']['links_missing_details'] == 2
    assert report['candidates'][0]['classification'] == 'manual_review'


def test_old_fresh_marker_does_not_mean_current_evidence():
    db, ibkr = fixture()
    ibkr[0]['_agm_refresh']['last_success_at'] = (datetime.now(timezone.utc) - timedelta(days=6)).isoformat()
    pair = dedup.build_report(db, ibkr)['candidates'][0]
    assert pair['classification'] == 'manual_review'
    assert 'old_or_undated_ibkr_evidence' in pair['evidence']


def test_unlinked_contacts_and_missing_details_are_visible():
    db, ibkr = fixture()
    db['account_contacts'] = db['account_contacts'][:1]
    report = dedup.build_report(db, [])
    assert report['coverage']['contacts_without_links'] == 1
    assert report['coverage']['accounts_without_ibkr_details'] == 1
    assert report['coverage']['contacts_without_resolved_identity'] == 2


def test_same_name_or_shared_email_is_not_strong_evidence():
    db, ibkr = fixture()
    db['account_contacts'] = []
    db['contacts'][1]['email'] = db['contacts'][0]['email']
    pair = dedup.build_report(db, ibkr)['candidates'][0]
    assert pair['classification'] == 'manual_review'


@pytest.mark.parametrize('shared_field', ['phone', 'email'])
def test_shared_household_details_never_create_a_candidate(shared_field):
    db, ibkr = fixture()
    db['account_contacts'] = []
    db['contacts'][0].update(name='Marco Monge', phone='5551234567', email='family@example.test')
    db['contacts'][1].update(name='Miguel Alfaro', phone='5557654321', email='other@example.test')
    db['contacts'][1][shared_field] = db['contacts'][0][shared_field]
    assert dedup.build_report(db, ibkr)['candidates'] == []


def test_abbreviated_name_with_missing_middle_and_second_surname_is_a_lead():
    db, ibkr = fixture()
    db['account_contacts'] = []
    db['contacts'][0]['name'] = 'Edgar Maradiaga'
    db['contacts'][1]['name'] = 'Edgar Ruben Maradiaga Cobos'
    pair = dedup.build_report(db, ibkr)['candidates'][0]
    assert 'compatible_abbreviated_name' in pair['evidence']


def test_shared_surnames_with_different_first_names_are_not_a_name_match():
    db, ibkr = fixture()
    db['account_contacts'] = []
    db['contacts'][0]['name'] = 'Juliana Castro Lapeira'
    db['contacts'][1]['name'] = 'Jimena Castro Lapeira'
    assert dedup.build_report(db, ibkr)['candidates'] == []


def test_different_company_registration_numbers_override_legal_form_similarity():
    db, ibkr = fixture()
    db['account_contacts'] = []
    db['contacts'][0]['name'] = '3-102-899563 Sociedad de Responsabilidad Limitada'
    db['contacts'][1]['name'] = '3-102-939900 Sociedad de Responsabilidad Limitada'
    assert dedup.build_report(db, ibkr)['candidates'] == []


def test_global_entity_id_finds_pair_on_different_accounts_without_other_signals():
    db, ibkr = fixture()
    db['accounts'].append({'id': 'a2', 'ibkr_account_number': 'U200', 'master_account': 'F200'})
    db['account_contacts'][1]['account_id'] = 'a2'
    db['contacts'][1]['name'] = 'Different Stored Label'
    ibkr[0]['associatedPersons'][0].pop('dateOfBirth')
    ibkr[0]['associatedPersons'][0]['identityDocuments'] = []
    other = copy.deepcopy(ibkr[0])
    other['account'].update(accountId='U200', masterAccountId='F200')
    report = dedup.build_report(db, ibkr + [other])
    assert report['candidates'] == []
    pair = report['identity_link_anomalies'][0]
    assert pair['evidence'] == ['same_ibkr_entity_across_accounts']
    assert pair['shared_ibkr_entity_ids'] == ['101']
    assert pair['shared_account_ids'] == []
    assert pair['classification'] == 'manual_review'


def test_cross_account_external_code_is_not_global_identity():
    db, ibkr = fixture()
    db['accounts'].append({'id': 'a2', 'ibkr_account_number': 'U200', 'master_account': 'F100'})
    db['account_contacts'][1].update(account_id='a2', entity_id=202)
    ibkr[0]['associatedPersons'][0]['externalCode'] = 'same-external'
    other = copy.deepcopy(ibkr[0])
    other['account']['accountId'] = 'U200'
    other['associatedPersons'][0]['entityId'] = 202
    pair = dedup.build_report(db, ibkr + [other])['candidates'][0]
    assert pair['shared_ibkr_entity_ids'] == []
    assert 'same_ibkr_entity_across_accounts' not in pair['evidence']


def test_repeated_document_number_with_other_country_is_not_same_identity():
    db, ibkr = fixture()
    db['accounts'].append({'id': 'a2', 'ibkr_account_number': 'U200', 'master_account': 'F100'})
    db['account_contacts'][1].update(account_id='a2', entity_id=202)
    other = copy.deepcopy(ibkr[0])
    other['account']['accountId'] = 'U200'
    other['associatedPersons'][0]['entityId'] = 202
    other['associatedPersons'][0]['identityDocuments'][0]['country'] = 'USA'
    ibkr.append(other)
    pair = dedup.build_report(db, ibkr)['candidates'][0]
    assert 'same_government_id_type_country' not in pair['evidence']
    assert pair['classification'] == 'manual_review'


def test_different_contact_type_blocks_strong_candidate():
    db, ibkr = fixture()
    db['contacts'][1]['type'] = 'company'
    pair = dedup.build_report(db, ibkr)['candidates'][0]
    assert 'different_contact_types' in pair['conflicts']


def test_company_stays_linked_without_inheriting_associated_person_identity():
    db, ibkr = fixture()
    db['contacts'][1]['type'] = 'company'
    report = dedup.build_report(db, ibkr)
    company = next(c for c in report['contacts'] if c['id'] == 'c2')
    assert company['account_ids'] == ['a1']
    assert company['ibkr_sources'] == []
    assert company['resolved_birthdays'] == []
    assert company['resolved_government_ids'] == []
    assert report['coverage']['company_links_with_person_entity_id'] == 1
    assert report['candidates'][0]['shared_ibkr_entity_ids'] == []


def test_input_validation_rejects_duplicate_ids_and_wrong_details_shape():
    db, ibkr = fixture()
    with pytest.raises(ValueError):
        dedup.build_report(db, {'outcomes': []})
    db['contacts'].append(db['contacts'][0])
    with pytest.raises(ValueError):
        dedup.build_report(db, ibkr)


def test_cli_is_offline_private_and_refuses_overwrite(tmp_path):
    db, ibkr = fixture()
    db_path, ibkr_path, output = (tmp_path / p for p in ('db.json', 'ibkr.json', 'report.json'))
    db_path.write_text(json.dumps(db))
    ibkr_path.write_text(json.dumps(ibkr))
    before = (db_path.read_bytes(), ibkr_path.read_bytes())
    command = [sys.executable, '-I', str(SCRIPT), '--snapshot', str(db_path), '--ibkr-details', str(ibkr_path), '--output', str(output)]
    run = subprocess.run(command, capture_output=True, text=True, check=True, env={})
    assert 'Ana' not in run.stdout
    assert output.stat().st_mode & 0o777 == 0o600
    report_bytes = output.read_bytes()
    assert json.loads(report_bytes)['input_sha256']
    assert subprocess.run(command, capture_output=True).returncode == 1
    assert output.read_bytes() == report_bytes
    assert before == (db_path.read_bytes(), ibkr_path.read_bytes())
