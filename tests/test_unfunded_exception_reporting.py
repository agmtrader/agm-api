"""Actual batch/Gmail/Flask failure path with synthetic data and no provider calls."""
import ast
import logging
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


def test_timeout_bubbles_once_and_stops_batch(batch_client, caplog):
    client, execute, connector = batch_client
    execute.side_effect = TimeoutError('synthetic Gmail timeout')
    response = client.get('/actions/send_unfunded_emails?token=excluded-query-marker')
    assert response.status_code == 500
    assert response.json['error'] == 'Internal server error'
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
    assert execute.call_count == 1  # No second recipient or new automatic retry.
    connector._force_connection_refresh.assert_not_called()
    assert not any('Unfunded email sent successfully:' in r.getMessage() for r in caplog.records)


def test_controlled_failure_keeps_guidance_without_exception_report(batch_client, caplog):
    client, execute, _ = batch_client
    execute.side_effect = errors.ServiceError('Correct synthetic input', 422, code='validation_error')
    response = client.get('/actions/send_unfunded_emails')
    assert response.status_code == 422
    assert response.json['error'] == 'Correct synthetic input'
    assert response.json['error_id'] == 'synthetic-batch-request'
    assert not any(r.levelno >= logging.ERROR for r in caplog.records)
    assert execute.call_count == 1


def test_success_preserves_batch_results_without_failure_reports(batch_client, caplog):
    client, execute, _ = batch_client
    response = client.get('/actions/send_unfunded_emails')
    assert response.status_code == 200
    assert len(response.json) == 2
    assert execute.call_count == 2
    assert not any(r.levelno >= logging.WARNING for r in caplog.records)
