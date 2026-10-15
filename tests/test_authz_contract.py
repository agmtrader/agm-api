"""Real Flask/JWT/middleware boundaries with synthetic identity storage only."""
from flask import Flask, g
from flask_jwt_extended import JWTManager, create_access_token
import pytest
from src.utils import authz


@pytest.fixture
def client(monkeypatch):
    app=Flask(__name__);app.config['JWT_SECRET_KEY']='synthetic-test-signing-key-at-least-32-bytes'
    JWTManager(app)
    users={'limited':{'id':'limited','email':'limited@example.test','scopes':'accounts/read'},'full':{'id':'full','email':'full@example.test','scopes':'all'},'advisor':{'id':'advisor','email':'advisor@example.test','scopes':'advisor'}}
    monkeypatch.setattr(authz,'_load_user',lambda identity:users.get(identity))
    @app.before_request
    def context():g.request_id='fixture-request'
    app.before_request(authz.authenticate_request);app.before_request(authz.enforce_route_scope)
    app.add_url_rule('/accounts/read','read',lambda:{'ok':True})
    app.add_url_rule('/accounts/ibkr/withdraw','withdraw',lambda:{'ok':True},methods=['POST'])
    def token(identity,claims=None):
        with app.app_context():return create_access_token(identity=identity,additional_claims=claims or {})
    return app.test_client(),token


def test_anonymous_invalid_and_unknown_identity_are_401(client):
    http,token=client
    for headers in [{},{'Authorization':'Bearer invalid'},{'Authorization':'Bearer '+token('deleted')}]:
        assert http.get('/accounts/read',headers=headers).status_code==401


def test_verified_identity_scope_overrides_forged_claims_and_parameters(client):
    http,token=client;headers={'Authorization':'Bearer '+token('limited',{'scopes':'all'})}
    assert http.get('/accounts/read',headers=headers).status_code==200
    assert http.post('/accounts/ibkr/withdraw?user_id=full&scope=all',headers=headers,json={'user_id':'full'}).status_code==403
    assert http.post('/accounts/ibkr/withdraw',headers={'Authorization':'Bearer '+token('full')}).status_code==200


def test_actual_advisor_quote_route_filters_before_enrichment(client,monkeypatch):
    from src.app.clients.advisors import bp
    from src.components.clients import advisors
    http,token=client
    http.application.register_blueprint(bp,url_prefix='/advisors')
    monkeypatch.setattr(advisors,'get_current_advisor',lambda:{'code':7})
    monkeypatch.setattr(authz,'get_current_advisor',lambda:{'code':7})
    calls=[]
    def owned(query):
        calls.append(query);return [{'id':'own','ibkr_account_number':'U-OWN'}]
    monkeypatch.setattr(advisors,'read_accounts',owned)
    monkeypatch.setattr(advisors,'get_open_positions_report',lambda:[{'ClientAccountID':'U-OWN','OpenDateTime':'20261010'},{'ClientAccountID':'U-OTHER','OpenDateTime':'20261010'}])
    def enrich(rows):
        assert len(rows)==1 and rows[0]['ClientAccountID']=='U-OWN'
        return [{**row,'Bid':99} for row in rows]
    monkeypatch.setattr(advisors,'add_open_position_quotes',enrich)
    response=http.get('/advisors/me/open_positions?quotes=1&advisor_code=999',headers={'Authorization':'Bearer '+token('advisor')})
    assert response.status_code==200
    assert response.json==[{'ClientAccountID':'U-OWN','OpenDateTime':'20261010','Bid':99}]
    assert calls==[{'advisor_code':7}]


@pytest.mark.parametrize('dependency', ['user', 'advisor'])
def test_identity_store_outage_returns_503_without_executing_route(monkeypatch, dependency):
    from src.utils.exception import ServiceError, build_error_payload
    app = Flask(__name__)
    app.config['JWT_SECRET_KEY'] = 'synthetic-test-signing-key-at-least-32-bytes'
    JWTManager(app)
    app.before_request(authz.authenticate_request)
    app.before_request(authz.enforce_route_scope)
    reached = []
    def route():
        reached.append(True)
        return {'ok': True}
    app.add_url_rule('/advisors/me/accounts', 'advisor_accounts', route)
    @app.errorhandler(ServiceError)
    def unavailable(exc):
        return build_error_payload(exc), exc.status_code
    def read(table, query):
        if dependency == 'user' or table != 'user':
            raise ServiceError('Database temporarily unavailable', status_code=503, code='database_unavailable')
        return [{'id': 'synthetic', 'email': 'advisor@example.test', 'scopes': 'advisor'}]
    class IdentityStore:
        pass
    store = IdentityStore()
    store.read = read
    monkeypatch.setattr(authz, 'db', store)
    with app.app_context():
        token = create_access_token(identity='synthetic')
    response = app.test_client().get('/advisors/me/accounts', headers={'Authorization': 'Bearer ' + token})
    assert response.status_code == 503
    assert response.json['code'] == 'database_unavailable'
    assert reached == []
