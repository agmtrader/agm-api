"""Synthetic broker/DB only; exercise the real route, auth, validation and signed PATCH."""
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from flask import Flask
from flask_jwt_extended import JWTManager, create_access_token
from src.utils.exception import ServiceError
from src.utils import authz
from src.components.clients import accounts
from src.app.clients.accounts import bp
from src.utils.connectors import ibkr_web_api as connector

INTERNAL_ID = '00000000-0000-4000-8000-000000000001'
BROKER_ID = 'U-SYNTHETIC'
MASTER = 'I6413690'
PERSON = {'entityId': 123, 'externalCode': 'external-1', 'firstName': 'Jane', 'lastName': 'Old',
          'associations': ['Account Holder'], 'residence': {'street1': '1 Main', 'city': 'Test City', 'country': 'USA', 'postalCode': '12345'},
          'username': 'must-not-leak', 'email': 'private@example.test'}


@pytest.fixture
def fixture(monkeypatch):
    row = {'id': INTERNAL_ID, 'master_account': MASTER, 'ibkr_account_number': BROKER_ID}
    details = {'account': {'accountId': BROKER_ID, 'masterAccountId': MASTER, 'applicantType': 'Individual', 'clearingStatus': 'O'},
               'associatedPersons': [deepcopy(PERSON)]}
    db = Mock(); db.read.return_value = [row]
    broker = Mock(); broker.get_account_details.return_value = details
    broker.get_pending_tasks.return_value = {'pendingTasks': []}
    broker.change_account_holder_detail.return_value = {'status': 'PENDING', 'request_id': 'request-1', 'message': 'Accepted'}
    monkeypatch.setattr(accounts, 'db', db); monkeypatch.setattr(accounts, 'ibkr_web_api', broker)
    app = Flask(__name__); app.config['JWT_SECRET_KEY'] = 'synthetic-signing-key-at-least-32-characters'
    JWTManager(app); app.register_blueprint(bp, url_prefix='/accounts')
    monkeypatch.setattr(authz, '_load_user', lambda identity: {'id': identity, 'scopes': 'all' if identity == 'operator' else 'accounts/read'})
    app.before_request(authz.authenticate_request); app.before_request(authz.enforce_route_scope)
    with app.app_context():
        operator = {'Authorization': 'Bearer ' + create_access_token(identity='operator')}
        reader = {'Authorization': 'Bearer ' + create_access_token(identity='reader')}
    return SimpleNamespace(http=app.test_client(), operator=operator, reader=reader, row=row, details=details, db=db, broker=broker)


def submit(f, **extra):
    return f.http.post('/accounts/ibkr/account_holder_detail', headers=f.operator, json={
        'account_id': INTERNAL_ID, 'person_key': 'entity:123', 'changes': {'name': {'first': 'Jane', 'last': 'New'}}, **extra})


def test_route_targets_only_verified_holder_and_stored_master(fixture):
    f = fixture
    response = submit(f)
    assert response.status_code == 200 and response.json['status'] == 'PENDING'
    f.broker.change_account_holder_detail.assert_called_once_with(account_id=BROKER_ID, master_account=MASTER,
        holder_details={'id': '123', 'name': {'first': 'Jane', 'last': 'New'}})
    f.db.update.assert_not_called(); f.db.create.assert_not_called()
    context = f.http.get('/accounts/ibkr/account_holder_edit_context?account_id=' + INTERNAL_ID, headers=f.operator)
    assert context.json['eligible'] is True
    assert context.json['people'][0]['person_key'] == 'entity:123'
    assert 'username' not in context.json['people'][0] and 'email' not in context.json['people'][0]


def test_real_route_denies_anonymous_and_read_only_users(fixture):
    f = fixture
    for headers, status in [({}, 401), (f.reader, 403)]:
        assert f.http.post('/accounts/ibkr/account_holder_detail', headers=headers, json={}).status_code == status
    f.broker.get_account_details.assert_not_called(); f.broker.change_account_holder_detail.assert_not_called()


@pytest.mark.parametrize('account_type,status,task,allowed', [('Organization', 'O', False, False), ('Trust', 'O', False, False),
    ('Joint', 'P', False, False), ('Individual', 'N', True, True), ('Joint', 'P', True, True), ('Individual', 'C', True, False)])
def test_account_eligibility_is_rechecked_at_submission(fixture, account_type, status, task, allowed):
    f = fixture; f.details['account'].update(applicantType=account_type, clearingStatus=status)
    f.broker.get_pending_tasks.return_value = {'pendingTasks': [{'formNumber': 9974}] if task else []}
    assert submit(f).status_code == (200 if allowed else 409)
    assert f.broker.change_account_holder_detail.call_count == int(allowed)


@pytest.mark.parametrize('extra', [{'master_account': 'OTHER'}, {'referenceUserName': 'other'}, {'person_key': 'entity:other'},
    {'changes': {}}, {'changes': {'email': 'other@example.test'}}, {'changes': {'name': {'first': 'Jane', 'last': 'Old'}}},
    {'changes': {'dateOfBirth': '2026-02-30'}}, {'changes': {'residenceAddress': {'country': 'USA'}}},
    {'changes': {'phones': [{'type': 'Mobile', 'number': '12345', 'country': 'US'}]}},
    {'changes': {'phones': [{'type': [], 'number': '12345', 'country': 'USA'}]}}, {'account_id': 'invalid'}])
def test_invalid_or_noop_request_never_writes(fixture, extra):
    assert submit(fixture, **extra).status_code == 400
    fixture.broker.change_account_holder_detail.assert_not_called()


@pytest.mark.parametrize('kind', ['unknown_master', 'mismatched_master', 'trusted_contact', 'duplicate_person', 'dual_language'])
def test_identity_and_context_guards(fixture, kind):
    f = fixture
    if kind == 'unknown_master': f.row['master_account'] = 'UNKNOWN'
    elif kind == 'mismatched_master': f.details['account']['masterAccountId'] = 'OTHER'
    elif kind == 'trusted_contact': f.details['associatedPersons'][0]['associations'] = ['Trusted Contact']
    elif kind == 'duplicate_person': f.details['associatedPersons'].append(deepcopy(PERSON))
    else: f.row['application_json'] = {'customer': {'translation': True}}
    assert submit(f).status_code in {400, 409}
    f.broker.change_account_holder_detail.assert_not_called()
    if kind == 'unknown_master': f.broker.get_account_details.assert_not_called()


def test_external_identity_is_resolved_from_fresh_details(fixture):
    fixture.details['associatedPersons'][0].pop('entityId')
    assert submit(fixture, person_key='external:external-1').status_code == 200
    assert fixture.broker.change_account_holder_detail.call_args.kwargs['holder_details']['externalId'] == 'external-1'


@pytest.mark.parametrize('changes', [
    {'residenceAddress': {'street1': '2 Main', 'city': 'Test City', 'country': 'USA', 'postalCode': '12345'}},
    {'phones': [{'type': 'Mobile', 'number': '1112223333', 'country': 'USA'}, {'type': 'Home', 'number': '9876543210', 'country': 'USA'}]},
])
def test_valid_address_and_phone_groups_preserve_requested_shape(fixture, changes):
    assert submit(fixture, changes=changes).status_code == 200
    assert fixture.broker.change_account_holder_detail.call_args.kwargs['holder_details'] == {'id': '123', **changes}


def result(status='PENDING', **extra):
    return {'requestId': 'outer', 'fileData': {'data': {'changeAccountHolderDetail': [
        {'status': status, 'requestId': 'inner', 'referenceAccountId': BROKER_ID, **extra}]}}}


@pytest.mark.parametrize('payload,expected', [(result(), 200), (result('PROCESSED'), 200),
    (result('ERROR', error=[{'value': 'Supporting documents required'}]), 422), (result('REJECTED'), 422),
    (result('PENDING', error=[{'value': 'Invalid field'}]), 422), (result('NEW_STATUS'), 502),
    ({}, 502), (result(referenceAccountId='OTHER'), 502)])
def test_connector_signs_account_scoped_request_and_parses_business_result(monkeypatch, payload, expected):
    monkeypatch.setattr(connector, 'get_secret', lambda _: 'synthetic-key')
    api = connector.IBKRWebAPI(); original = (api.CLIENT_ID, api.KEY_ID, api.CLIENT_PRIVATE_KEY)
    api.get_bearer_token = Mock(return_value='synthetic-token'); api.sign_request = Mock(return_value='synthetic-jwt')
    patch = Mock(return_value=SimpleNamespace(status_code=200, json=lambda: payload)); monkeypatch.setattr(connector.requests, 'patch', patch)
    fields = {'id': '123', 'name': {'first': 'Jane', 'last': 'New'}}
    if expected == 200:
        assert api.change_account_holder_detail(BROKER_ID, fields, 'F10740574')['status'] == payload['fileData']['data']['changeAccountHolderDetail'][0]['status']
    else:
        with pytest.raises(ServiceError) as caught: api.change_account_holder_detail(BROKER_ID, fields, 'F10740574')
        assert caught.value.status_code == expected
    api.sign_request.assert_called_once_with({'accountManagementRequests': {'changeAccountHolderDetail': {'accountId': BROKER_ID, 'newAccountHolderDetails': [fields]}}})
    assert patch.call_args.kwargs['data'] == 'synthetic-jwt' and patch.call_count == 1
    assert patch.call_args.kwargs['headers']['Content-Type'] == 'application/jwt'
    assert (api.CLIENT_ID, api.KEY_ID, api.CLIENT_PRIVATE_KEY) == original


def test_timeout_is_not_retried_and_restores_credentials(monkeypatch):
    monkeypatch.setattr(connector, 'get_secret', lambda _: 'synthetic-key')
    api = connector.IBKRWebAPI(); api.get_bearer_token = lambda: 'synthetic-token'; api.sign_request = lambda _: 'synthetic-jwt'
    patch = Mock(side_effect=connector.requests.Timeout('synthetic timeout')); monkeypatch.setattr(connector.requests, 'patch', patch)
    with pytest.raises(ServiceError): api.change_account_holder_detail(BROKER_ID, {'id': '123', 'dateOfBirth': '1980-01-01'}, 'F10740574')
    assert patch.call_count == 1 and api.CLIENT_ID == 'AGMTechnology-FD2'


def test_unreadable_response_is_uncertain_without_retry(monkeypatch):
    monkeypatch.setattr(connector, 'get_secret', lambda _: 'synthetic-key')
    api = connector.IBKRWebAPI(); api.get_bearer_token = lambda: 'synthetic-token'; api.sign_request = lambda _: 'synthetic-jwt'
    patch = Mock(return_value=SimpleNamespace(status_code=200, json=Mock(side_effect=ValueError('invalid json'))))
    monkeypatch.setattr(connector.requests, 'patch', patch)
    with pytest.raises(ServiceError) as caught:
        api.change_account_holder_detail(BROKER_ID, {'id': '123', 'dateOfBirth': '1980-01-01'}, 'F10740574')
    assert caught.value.code == 'ibkr_holder_result_unknown' and 'before resubmitting' in str(caught.value)
    assert patch.call_count == 1 and api.CLIENT_ID == 'AGMTechnology-FD2'
