"""Real statement routes/auth/connector; only identity storage and IBKR HTTP are fixtures."""
import base64
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from flask import Flask
from flask_jwt_extended import JWTManager, create_access_token
from requests import Response

from src.utils import authz
from src.utils.connectors import ibkr_web_api as connector
from src.components.clients import accounts, advisors
from src.app.clients.accounts import bp as accounts_bp
from src.app.clients.advisors import bp as advisors_bp

# Observed live on 2026-10-15: HTTP 200, text/plain, 233 bytes. No customer data.
UNAVAILABLE = {
    'type': '/simple', 'title': 'OK', 'status': 200,
    'detail': 'Daily statements are not produced for account(s) awaiting initial funding. '
              'Monthly statements are provided for all accounts regardless of funding state.',
    'instance': '/statements',
}
PDF = b'%PDF-1.4\nfixture statement bytes\n%%EOF'
ROW = {'id': '00000000-0000-4000-8000-000000000001',
       'ibkr_account_number': 'U-SANITIZED', 'master_account': 'F10740574', 'advisor_code': 7}


def response(body, content_type, status=200):
    result = Response()
    result.status_code = status
    result.headers['Content-Type'] = content_type
    result._content = body if isinstance(body, bytes) else json.dumps(body, separators=(',', ':')).encode()
    return result


@pytest.fixture
def flow(monkeypatch):
    monkeypatch.setattr(connector, 'get_secret', lambda _: 'synthetic-private-key')
    broker = connector.IBKRWebAPI()
    monkeypatch.setattr(broker, 'get_bearer_token', lambda: 'synthetic-token')
    signed = []
    monkeypatch.setattr(broker, 'sign_request', lambda body: signed.append(body) or 'synthetic-signature')
    post = Mock(return_value=response(UNAVAILABLE, 'text/plain;charset=UTF-8'))
    success = Mock()
    monkeypatch.setattr(connector.logger, 'success', success)
    monkeypatch.setattr(connector.requests, 'post', post)
    monkeypatch.setattr(accounts, 'ibkr_web_api', broker)
    monkeypatch.setattr(advisors, 'read_accounts', lambda query: [ROW] if query == {'advisor_code': 7} else [])
    monkeypatch.setattr(authz, '_load_user', lambda identity: {'id': identity, 'scopes': 'all' if identity == 'operator' else 'advisor'})
    monkeypatch.setattr(authz, 'get_current_advisor', lambda: {'code': 7})
    monkeypatch.setattr(advisors, 'get_current_advisor', lambda: {'code': 7})
    app = Flask(__name__)
    app.config['JWT_SECRET_KEY'] = 'synthetic-signing-key-at-least-32-characters'
    JWTManager(app)
    app.before_request(authz.authenticate_request)
    app.before_request(authz.enforce_route_scope)
    app.register_blueprint(accounts_bp, url_prefix='/accounts')
    app.register_blueprint(advisors_bp, url_prefix='/advisors')
    with app.app_context():
        headers = {role: {'Authorization': 'Bearer ' + create_access_token(identity=role)} for role in ('operator', 'advisor')}
    return SimpleNamespace(http=app.test_client(), headers=headers, post=post, signed=signed, success=success)


def download(flow, role='operator', start='20260901', end='20260930', account=None):
    advisor = role == 'advisor'
    return flow.http.post('/advisors/me/account_statement' if advisor else '/accounts/ibkr/statements',
        headers=flow.headers[role], json={
            'account_id': account or (ROW['id'] if advisor else ROW['ibkr_account_number']),
            'master_account': 'IGNORED-CALLER-MASTER' if advisor else ROW['master_account'],
            'start_date': start, 'end_date': end, 'language': 'en',
        })


@pytest.mark.parametrize('role', ['operator', 'advisor'])
def test_observed_http_200_text_problem_becomes_actionable_unavailable(flow, role):
    assert len(flow.post.return_value.content) == 233
    result = download(flow, role)
    assert result.status_code == 409
    assert result.json['code'] == 'ibkr_statement_unavailable'
    assert result.json['error'] == UNAVAILABLE['detail']
    assert 'data' not in result.json
    flow.success.assert_not_called()


@pytest.mark.parametrize('role', ['operator', 'advisor'])
@pytest.mark.parametrize('raw_pdf', [False, True])
def test_monthly_request_and_pdf_contract_are_preserved(flow, role, raw_pdf):
    encoded = base64.b64encode(PDF).decode()
    flow.post.return_value = response(PDF, 'application/pdf') if raw_pdf else response(
        {'data': {'dataType': 'String', 'mimeType': 'application/pdf', 'encoding': 'base64', 'value': encoded}},
        'application/json')
    result = download(flow, role, start='202609', end='202609')
    assert result.status_code == 200
    assert result.json['data']['mimeType'] == 'application/pdf'
    assert base64.b64decode(result.json['data']['value']) == PDF
    flow.success.assert_called_once_with('Statements fetched successfully')
    assert flow.signed == [{'accountId': ROW['ibkr_account_number'], 'startDate': '202609',
                            'endDate': '202609', 'mimeType': 'application/pdf', 'language': 'en'}]


@pytest.mark.parametrize('body,content_type', [
    (b'<html>Not a statement</html>', 'text/html'),
    ({'data': {'value': 'not-base64', 'mimeType': 'application/pdf'}}, 'application/json'),
    ({'data': {'value': base64.b64encode(b'not a PDF').decode(), 'mimeType': 'application/pdf'}}, 'application/json'),
    ({'data': None}, 'application/json'),
])
def test_non_documents_are_never_successful_statements(flow, body, content_type):
    flow.post.return_value = response(body, content_type)
    result = download(flow)
    assert result.status_code == 502
    assert result.json['code'] == 'ibkr_statement_invalid'


def test_advisor_cannot_download_another_account_or_use_admin_route(flow):
    assert download(flow, 'advisor', account='other-account').status_code == 404
    assert flow.http.post('/accounts/ibkr/statements', headers=flow.headers['advisor'], json={}).status_code == 403
    flow.post.assert_not_called()
