"""Actual batch/Gmail/Flask failure path with synthetic data and no provider calls."""
import ast
import logging
import json
import re
import time
import types
from functools import wraps
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
from flask import Flask, g
from src.utils import exception as errors
from src.utils.response import format_response

ROOT = Path(__file__).resolve().parents[1]


def source_function(path, name, namespace, *, class_name=None):
    tree = ast.parse((ROOT / path).read_text())
    nodes = tree.body
    if class_name:
        nodes = next(n for n in nodes if isinstance(n, ast.ClassDef) and n.name == class_name).body
    node = next(n for n in nodes if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(ROOT / path), 'exec'), namespace)
    return namespace[name]


@pytest.fixture
def batch_client(monkeypatch, caplog):
    log = logging.getLogger('unfunded-reporting-acceptance')
    caplog.set_level(logging.INFO, logger=log.name)
    monkeypatch.setattr(errors.logger, 'logger', log)
    gmail_namespace = {'logger': errors.logger, 'handle_exception': errors.handle_exception,
                       'wraps': wraps, 'time': time}
    source_function('src/utils/connectors/gmail.py', 'retry_on_connection_error', gmail_namespace)
    send = source_function('src/utils/connectors/gmail.py', 'send_email', gmail_namespace,
                           class_name='GmailConnector')
    execute = Mock(return_value={'id': 'synthetic-message'})
    service = Mock()
    service.users.return_value.messages.return_value.send.return_value.execute = execute
    connector = types.SimpleNamespace(service=service, create_html_email=Mock(return_value={'raw': 'synthetic'}),
                                      _ensure_fresh_connection=Mock(), _force_connection_refresh=Mock())

    @errors.handle_exception
    def funding_notification(**kwargs):
        return send(connector, kwargs['content'], kwargs['client_email'], 'synthetic subject',
                    'funding_notification_es', bcc='', cc=kwargs['cc'])

    accounts = types.ModuleType('src.components.clients.accounts')
    accounts.read_accounts = Mock(return_value=[
        {'id': 'a1', 'ibkr_account_number': 'SYNTHETIC1', 'contact_id': 'c1', 'advisor_code': 1},
        {'id': 'a2', 'ibkr_account_number': 'SYNTHETIC2', 'contact_id': 'c2', 'advisor_code': 1},
    ])
    accounts.read_account_contacts = Mock(return_value=[])
    accounts.send_funding_notification_email = funding_notification
    contacts = types.ModuleType('src.components.clients.contacts')
    contacts.read_contacts = Mock(return_value=[
        {'id': 'c1', 'name': 'Synthetic One', 'email': 'one@example.test'},
        {'id': 'c2', 'name': 'Synthetic Two', 'email': 'two@example.test'},
        {'id': 'ad', 'name': 'Synthetic Advisor', 'email': 'advisor@example.test'},
    ])
    advisors = types.ModuleType('src.components.clients.advisors')
    advisors.read_advisors = Mock(return_value=[{'code': 1, 'contact_id': 'ad'}])
    for module in (accounts, contacts, advisors):
        monkeypatch.setitem(__import__('sys').modules, module.__name__, module)
    namespace = {'pd': pd, 'logger': errors.logger, 'handle_exception': errors.handle_exception,
                 're': re, 'ServiceError': errors.ServiceError, 'log_service_error': errors.log_service_error,
                 'ensure_error_id': errors.ensure_error_id,
                 'wrap_unhandled_exception': errors.wrap_unhandled_exception,
                 'UNFUNDED_EMAIL_EXCLUSIONS': frozenset(), 'UNFUNDED_ACCOUNT_EXCLUSIONS': frozenset(),
                 'NAV_ZERO_DISPLAY_THRESHOLD': 1,
                 'get_nav_report': lambda: [{'ClientAccountID': 'SYNTHETIC1', 'Total': 0},
                                           {'ClientAccountID': 'SYNTHETIC2', 'Total': 0}],
                 'get_clients_report': lambda: [
                     {'Account ID': a, 'Status': 'Open', 'Date Opened': '2026-10-01'}
                     for a in ('SYNTHETIC1', 'SYNTHETIC2')]}
    operation = source_function('src/components/tools/private/actions.py', 'send_unfunded_emails', namespace)
    app = Flask(__name__)
    app.before_request(lambda: setattr(g, 'request_id', 'synthetic-batch-request'))
    app.add_url_rule('/actions/send_unfunded_emails', 'unfunded', format_response(operation))
    return app.test_client(), execute, connector


def test_timeout_reports_once_and_continues_batch(batch_client, caplog):
    client, execute, connector = batch_client
    execute.side_effect = [TimeoutError('synthetic Gmail timeout'), {'id': 'second-message'}]
    response = client.get('/actions/send_unfunded_emails?token=excluded-query-marker')
    assert response.status_code == 200
    assert response.json['status'] == 'partial'
    assert response.json['summary'] == {'total': 2, 'sent': 1, 'failed': 1, 'skipped': 0}
    failed, sent = response.json['recipients']
    assert failed['error'] == 'Internal server error'
    assert failed['error_id']
    assert failed['delivery_status'] == 'unknown'
    assert sent['email'] == 'two@example.test'
    assert sent['message_id'] == 'second-message'
    reports = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(reports) == 1
    report = reports[0].getMessage()
    assert 'ServiceError: Internal server error' in report
    assert 'synthetic Gmail timeout' not in report
    assert report.count('Traceback (most recent call last):') == 1
    assert 'excluded-query-marker' not in report
    assert 'one@example.test' not in report
    assert '.execute()' not in report
    assert reports[0].exc_info is None
    assert sum(r.levelno == logging.WARNING and 'Unfunded email failed:' in r.getMessage()
               for r in caplog.records) == 1
    assert execute.call_count == 2  # Later recipient attempted; failed send not retried.
    connector._force_connection_refresh.assert_not_called()
    assert sum('Successfully sent funding_notification_es' in r.getMessage() for r in caplog.records) == 1


def test_controlled_failure_keeps_guidance_without_exception_report(batch_client, caplog):
    client, execute, _ = batch_client
    execute.side_effect = [errors.ServiceError('Correct synthetic input', 422, code='validation_error'),
                           {'id': 'second-message'}]
    response = client.get('/actions/send_unfunded_emails')
    assert response.status_code == 200
    assert response.json['status'] == 'partial'
    assert response.json['recipients'][0]['error'] == 'Correct synthetic input'
    assert response.json['recipients'][0]['error_status'] == 422
    assert response.json['recipients'][0]['error_id'] == 'synthetic-batch-request'
    assert not any(r.levelno >= logging.ERROR for r in caplog.records)
    assert execute.call_count == 2


def test_success_preserves_batch_results_without_failure_reports(batch_client, caplog):
    client, execute, _ = batch_client
    response = client.get('/actions/send_unfunded_emails')
    assert response.status_code == 200
    assert response.json['status'] == 'success'
    assert response.json['summary'] == {'total': 2, 'sent': 2, 'failed': 0, 'skipped': 0}
    assert execute.call_count == 2
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)


def test_all_failed_attempts_every_recipient(batch_client, caplog):
    client, execute, _ = batch_client
    execute.side_effect = TimeoutError('synthetic timeout')
    response = client.get('/actions/send_unfunded_emails')
    assert response.status_code == 200
    assert response.json['status'] == 'failed'
    assert response.json['summary'] == {'total': 2, 'sent': 0, 'failed': 2, 'skipped': 0}
    assert execute.call_count == 2
    assert sum(r.levelno >= logging.ERROR for r in caplog.records) == 2


@pytest.mark.parametrize('email', [None, '', 'missing-at-sign', 'one@example.test\r\nBcc:other@example.test'])
def test_invalid_email_is_skipped_and_nulls_are_valid_json(batch_client, email):
    client, execute, _ = batch_client
    import sys
    records = sys.modules['src.components.clients.contacts'].read_contacts.return_value
    records[0]['email'] = email
    records[0]['name'] = float('nan')
    response = client.get('/actions/send_unfunded_emails')
    assert response.json['summary'] == {'total': 2, 'sent': 1, 'failed': 0, 'skipped': 1}
    assert response.json['recipients'][0]['name'] is None
    json.dumps(response.json, allow_nan=False)
    assert execute.call_count == 1


def test_opt_out_is_skipped_case_insensitively(batch_client):
    client, execute, _ = batch_client
    operation = client.application.view_functions['unfunded'].__wrapped__.__wrapped__
    operation.__globals__['UNFUNDED_EMAIL_EXCLUSIONS'] = frozenset({'one@example.test'})
    import sys
    sys.modules['src.components.clients.contacts'].read_contacts.return_value[0]['email'] = ' ONE@EXAMPLE.TEST '
    response = client.get('/actions/send_unfunded_emails')
    assert response.json['recipients'][0]['reason'] == 'Explicit email exclusion'
    assert response.json['summary']['skipped'] == 1
    assert execute.call_count == 1


def test_account_exclusion_is_never_sent(batch_client):
    client, execute, _ = batch_client
    operation = client.application.view_functions['unfunded'].__wrapped__.__wrapped__
    operation.__globals__['UNFUNDED_ACCOUNT_EXCLUSIONS'] = frozenset({'SYNTHETIC1'})
    response = client.get('/actions/send_unfunded_emails')
    assert response.json['summary']['total'] == 1
    assert response.json['recipients'][0]['ibkr_account_number'] == 'SYNTHETIC2'
    assert execute.call_count == 1


def test_preparation_failure_remains_request_failure_without_sends(batch_client):
    client, execute, _ = batch_client
    import sys
    sys.modules['src.components.clients.accounts'].read_accounts.side_effect = errors.ServiceError('Report unavailable', 503)
    response = client.get('/actions/send_unfunded_emails')
    assert response.status_code == 503
    execute.assert_not_called()


def test_no_candidates_returns_empty_success(batch_client):
    client, execute, _ = batch_client
    operation = client.application.view_functions['unfunded'].__wrapped__.__wrapped__
    operation.__globals__['UNFUNDED_ACCOUNT_EXCLUSIONS'] = frozenset({'SYNTHETIC1', 'SYNTHETIC2'})
    response = client.get('/actions/send_unfunded_emails')
    assert response.status_code == 200
    assert response.json == {'status': 'success', 'summary': {'total': 0, 'sent': 0, 'failed': 0, 'skipped': 0},
                             'recipients': []}
    execute.assert_not_called()


@pytest.mark.parametrize('primary_email,secondary_email,expected', [
    (None, 'second@example.test', 'second@example.test'),
    ('first@example.test', 'second@example.test', 'first@example.test'),
    (None, None, None),
])
def test_requested_joint_holder_fallback_and_account_exclusion(batch_client, primary_email, secondary_email, expected):
    import inspect
    import sys
    from src.components.tools.private.actions import UNFUNDED_ACCOUNT_EXCLUSIONS
    client, execute, connector = batch_client
    ns = inspect.unwrap(client.application.view_functions['unfunded']).__globals__
    ns['UNFUNDED_ACCOUNT_EXCLUSIONS'] = UNFUNDED_ACCOUNT_EXCLUSIONS
    account_module = sys.modules['src.components.clients.accounts']
    account_module.read_accounts.return_value = [
        {'id': 'joint', 'ibkr_account_number': 'U28420921', 'contact_id': 'first', 'advisor_code': 1,
         'application_json': {'customer': {'jointHolders': {'secondHolderDetails': [{'externalId': 'second-holder'}]}}}},
        {'id': 'excluded', 'ibkr_account_number': 'U27090911', 'contact_id': 'second', 'advisor_code': 1}]
    account_module.read_account_contacts.return_value = [
        {'account_id': 'joint', 'contact_id': 'first', 'external_id': 'first-holder'},
        {'account_id': 'joint', 'contact_id': 'second', 'external_id': 'second-holder'},
        {'account_id': 'joint', 'contact_id': 'trusted', 'external_id': 'trusted-contact'}]
    sys.modules['src.components.clients.contacts'].read_contacts.return_value = [
        {'id': 'first', 'name': 'Synthetic First', 'email': primary_email},
        {'id': 'second', 'name': 'Synthetic Second', 'email': secondary_email},
        {'id': 'trusted', 'name': 'Synthetic Trusted', 'email': 'trusted@example.test'},
        {'id': 'ad', 'name': 'Synthetic Advisor', 'email': 'advisor@example.test'}]
    ns['get_nav_report'] = lambda: [{'ClientAccountID': n, 'Total': 0} for n in ('U28420921', 'U27090911')]
    ns['get_clients_report'] = lambda: [{'Account ID': n, 'Status': 'Open', 'Date Opened': '2026-10-01'} for n in ('U28420921', 'U27090911')]
    response = client.get('/actions/send_unfunded_emails')
    assert response.status_code == 200
    assert len(response.json['recipients']) == 1
    recipient = response.json['recipients'][0]
    assert recipient['ibkr_account_number'] == 'U28420921'
    assert recipient['email'] == expected
    assert recipient['status'] == ('sent' if expected else 'skipped')
    assert execute.call_count == (1 if expected else 0)
    if expected:
        assert connector.create_html_email.call_args.args[2] == expected
