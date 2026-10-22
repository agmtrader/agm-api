"""Real risk routes, declared ORM model and local SQLite storage; no external calls."""
import importlib.util
from pathlib import Path
import sys
import types
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest
from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import declarative_base

ROOT = Path(__file__).resolve().parents[1]


@compiles(JSONB, 'sqlite')
def sqlite_jsonb(element, compiler, **kwargs):
    return 'JSON'


def load(name, relative_path):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def client():
    logger = types.ModuleType('src.utils.logger')
    logger.logger = Mock()
    secrets = types.ModuleType('src.utils.managers.secret_manager')
    secrets.get_secret = Mock(side_effect=AssertionError('No secret reads in this fixture'))
    with patch.dict(sys.modules, {'src.utils.logger': logger, 'src.utils.managers.secret_manager': secrets}):
        errors = load('src.utils.exception', 'src/utils/exception.py')
        with patch.dict(sys.modules, {'src.utils.exception': errors}):
            connector = load('deposit_connector', 'src/utils/connectors/supabase.py')
            models = object.__new__(connector.Supabase)
            models.Base = declarative_base()
            models._setup_models()
            engine = create_engine('sqlite://')
            models.Base.metadata.create_all(engine, tables=[models.RiskProfile.__table__])
            manager_module = load('deposit_manager', 'src/utils/managers/database_manager.py')
            connector.db.set_target(manager_module.DatabaseManager(models.Base, engine))
            with patch.dict(sys.modules, {'src.utils.connectors.supabase': connector}):
                service = load('deposit_service', 'src/components/clients/risk_profiles.py')
                # Identity matching is already tested separately. Keep this fixture
                # focused on preserving the deposit even without a linked proposal.
                service._resolve_or_create_contact = Mock(return_value=None)
                response = load('src.utils.response', 'src/utils/response.py')
                with patch.dict(sys.modules, {
                    'src.components.clients.risk_profiles': service,
                    'src.utils.response': response,
                }):
                    routes = load('deposit_routes', 'src/app/clients/risk_profiles.py')
                    app = Flask(__name__)
                    app.register_blueprint(routes.bp, url_prefix='/risk_profiles')
                    yield app.test_client(), service, connector.db
            engine.dispose()


def payload(**extra):
    return {'name': 'Deposit QA', 'email': 'deposit@example.invalid', 'score': 1, 'answers': {'type': 'Conservative'}, **extra}


def test_deposit_survives_real_create_storage_and_read_without_any_proposal(client):
    api, service, db = client
    response = api.post('/risk_profiles/create', json={'data': payload(estimated_deposit=12345.67)})
    assert response.status_code == 200
    saved = db.read('risk_profile', {})[0]
    assert saved['estimated_deposit'] == Decimal('12345.67')
    assert saved['score'] == '1'
    result = api.get('/risk_profiles/read').get_json()[0]
    assert result['estimated_deposit'] == 12345.67
    assert result['assigned_risk_archetype'] == 'Conservative B'
    assert result['answers'] == {'type': 'Conservative'}


@pytest.mark.parametrize('amount', [0, -1, 0.001, '1.234', '', True, 'NaN', 'Infinity', '10000000000000000'])
def test_invalid_deposit_is_rejected_before_identity_or_storage_writes(client, amount):
    api, service, db = client
    response = api.post('/risk_profiles/create', json={'data': payload(estimated_deposit=amount)})
    assert response.status_code == 400
    assert response.get_json()['code'] == 'invalid_estimated_deposit'
    service._resolve_or_create_contact.assert_not_called()
    assert db.read('risk_profile', {}) == []


def test_older_clients_can_omit_deposit_without_inventing_an_amount(client):
    api, _, _ = client
    assert api.post('/risk_profiles/create', json={'data': payload()}).status_code == 200
    assert api.get('/risk_profiles/read').get_json()[0]['estimated_deposit'] is None
