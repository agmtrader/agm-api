"""Real PostgreSQL + real authenticated Flask route, with sanitized holder data."""
import os
import types
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock

import pytest
from flask import Flask
from flask_jwt_extended import JWTManager, create_access_token
from sqlalchemy import create_engine, select, text

from src.app.clients.contacts import bp
from src.components.clients import contact_identity
from src.utils import authz
from src.utils.exception import ServiceError


def holder(first='Gabriel', last='Example', number='001-234', birth='1990-01-01', email='shared@example.test', external='holder-one'):
    return {'name':{'first':first,'last':last},'email':email,'dateOfBirth':birth,'externalId':external,
            'identification':{'issuingCountry':'CRI','nationalCard':number}}


def application(person=None):
    return {'customer':{'type':'INDIVIDUAL','accountHolder':{'accountHolderDetails':[person or holder()]}}}


@pytest.fixture
def store():
    url = os.getenv('AGM_IDENTITY_TEST_DATABASE_URL')
    if not url:
        pytest.skip('Set AGM_IDENTITY_TEST_DATABASE_URL to an isolated PostgreSQL database')
    from sqlalchemy.engine import make_url
    parsed = make_url(url)
    assert parsed.database in {'agm_identity_fixture', 'agm_upload_fixture'} and parsed.host in {'localhost','127.0.0.1'}
    schema = 'agm_identity_test_' + uuid.uuid4().hex
    engine = create_engine(url, connect_args={'options': '-c search_path=' + schema})
    with engine.begin() as conn:
        conn.execute(text('create schema ' + schema))
        conn.execute(text('create table contact(id uuid primary key, created text, updated text, type text, name text not null, email text, phone text, met_place text)'))
        conn.execute(text('create table account(id uuid primary key, application_json jsonb, ibkr_account_number text)'))
        conn.execute(text('create table account_contact(id uuid primary key, account_id uuid not null references account(id), contact_id uuid not null references contact(id), entity_id bigint, external_id text, role text, created text, updated text)'))
        # Supabase roles do not exist in a plain disposable PostgreSQL cluster.
        for role in ('anon','authenticated','service_role'):
            if not conn.execute(text('select 1 from pg_roles where rolname=:role'),{'role':role}).scalar():
                conn.execute(text(f'create role {role}'))
        migration = next(Path('supabase/migrations').glob('*_contact_identity_matching.sql'))
        conn.execute(text(migration.read_text().replace('public.', schema + '.')))
    from sqlalchemy import MetaData
    metadata=MetaData();metadata.reflect(engine)
    yield types.SimpleNamespace(engine=engine,metadata=metadata)
    with engine.begin() as conn:conn.execute(text('drop schema ' + schema + ' cascade'))
    engine.dispose()


def create_account(store, payload=None):
    table=store.metadata.tables['account']; id=uuid.uuid4()
    with store.engine.begin() as conn: conn.execute(table.insert().values(id=id,application_json=payload or application()))
    return str(id)


def http_client(monkeypatch,store):
    from src.app.clients import contacts as routes
    from src.utils.connectors import supabase
    monkeypatch.setattr(supabase,'db',store)
    app=Flask(__name__);app.config['JWT_SECRET_KEY']='synthetic-identity-test-key-at-least-32-bytes';JWTManager(app)
    monkeypatch.setattr(authz,'_load_user',lambda identity:{'id':identity,'scopes':'contacts accounts/contact' if identity=='allowed' else 'accounts/read'})
    app.before_request(authz.authenticate_request);app.before_request(authz.enforce_route_scope)
    test_bp = bp
    if os.getenv('AGM_IDENTITY_ROUTES_SOURCE'):
        import runpy
        test_bp = runpy.run_path(os.environ['AGM_IDENTITY_ROUTES_SOURCE'])['bp']
    app.register_blueprint(test_bp,url_prefix='/contacts')
    with app.app_context(): token=create_access_token(identity='allowed');denied=create_access_token(identity='denied')
    return app.test_client(),{'Authorization':'Bearer '+token},{'Authorization':'Bearer '+denied}


def rows(store,table):
    with store.engine.connect() as conn:return list(conn.execute(select(store.metadata.tables[table])).mappings())


def test_real_route_persists_private_identity_and_reuses_contact_on_repeat(store,monkeypatch):
    id=create_account(store);http,headers,denied=http_client(monkeypatch,store)
    assert http.post('/contacts/reconcile_application',json={'account_id':id}).status_code==401
    assert http.post('/contacts/reconcile_application',json={'account_id':id},headers=denied).status_code==403
    response=http.post('/contacts/reconcile_application',json={'account_id':id},headers=headers)
    assert response.status_code==200
    assert response.json['linked_contacts'][0]['outcome']=='created'
    assert '1990-01-01' not in response.text and '001234' not in response.text
    second=http.post('/contacts/reconcile_application',json={'account_id':id},headers=headers)
    assert second.status_code==200 and second.json['linked_contacts'][0]['outcome']=='matched'
    assert len(rows(store,'contact'))==len(rows(store,'account_contact'))==len(rows(store,'contact_identity'))==1
    assert rows(store,'contact_identity_document')[0]['identifier']=='001234'


def reconcile(store,id):
    return contact_identity.reconcile_application_contacts(store,id)['linked_contacts']


def replace_payload(store,id,payload):
    t=store.metadata.tables['account']
    with store.engine.begin() as conn:conn.execute(t.update().where(t.c.id==uuid.UUID(id)).values(application_json=payload))


def test_changed_email_same_id_birthday_reuses_without_overwriting_contact(store):
    first=create_account(store);a=reconcile(store,first)
    other=create_account(store,application(holder(email='new@example.test',external='new-holder')))
    b=reconcile(store,other)
    assert a[0]['contact_id']==b[0]['contact_id']
    assert len(rows(store,'contact'))==1 and len(rows(store,'account_contact'))==2
    assert rows(store,'contact')[0]['email']=='shared@example.test'


def test_shared_email_and_same_name_different_ids_birthdays_remain_different_people(store):
    a=reconcile(store,create_account(store))
    b=reconcile(store,create_account(store,application(holder(number='00999',birth='1992-02-02'))))
    assert a[0]['contact_id']!=b[0]['contact_id']
    assert len(rows(store,'contact'))==2


def test_scoped_ids_preserve_leading_zeroes_country_and_document_type(store):
    a=reconcile(store,create_account(store))
    for change in ('leading-zero','country','type'):
        person=holder(first='Another',last=change,number='001234',birth='1992-02-02')
        if change=='leading-zero':person['identification']['nationalCard']='1234'
        elif change=='country':person['identification']['issuingCountry']='USA'
        else:person['identification']={'issuingCountry':'CRI','passport':'001234'}
        b=reconcile(store,create_account(store,application(person)))
        assert a[0]['contact_id']!=b[0]['contact_id']
    assert len(rows(store,'contact'))==4


def test_same_id_conflicting_birthday_rolls_back_all_writes_and_hides_values(store,monkeypatch):
    reconcile(store,create_account(store));id=create_account(store,application(holder(birth='1992-02-02')))
    http,headers,_=http_client(monkeypatch,store)
    response=http.post('/contacts/reconcile_application',headers=headers,json={'account_id':id})
    assert response.status_code==409 and response.json['code']=='contact_identity_conflict'
    assert '1992' not in response.text and '001234' not in response.text
    assert len(rows(store,'contact'))==len(rows(store,'account_contact'))==1


def test_joint_and_org_link_people_and_company_atomically_without_identity_collapse(store):
    people=[holder(first='Ana'),holder(first='Luis',number='9988',external='second')]
    joint={'customer':{'type':'JOINT','jointHolders':{'firstHolderDetails':[people[0]],'secondHolderDetails':[people[1]]}}}
    joint_result=reconcile(store,create_account(store,joint))
    org={'customer':{'type':'ORG','externalId':'company-id','organization':{'identifications':[{'name':'Example Company'}],
         'associatedEntities':{'associatedIndividuals':people}}}}
    id=create_account(store,org);result=reconcile(store,id)
    assert len(result)==3 and len(set(r['contact_id'] for r in result))==3
    assert result[0]['type']=='company' and result[0]['entityId'] is None
    assert {r['contact_id'] for r in result[1:]}=={r['contact_id'] for r in joint_result}
    reconcile(store,id)
    assert len(rows(store,'contact'))==3
    assert len(rows(store,'contact_identity'))==2
    assert len(rows(store,'account_contact'))==5


def test_later_holder_failure_rolls_back_company_and_earlier_holder(store):
    people=[holder(first='Ana'),holder(first='Luis',number='9988',birth='bad')]
    org={'customer':{'type':'ORG','organization':{'identifications':[{'name':'Example Company'}],
         'associatedEntities':{'associatedIndividuals':people}}}}
    with pytest.raises(ServiceError):reconcile(store,create_account(store,org))
    assert rows(store,'contact')==[] and rows(store,'account_contact')==[]


def test_existing_contact_without_identity_does_not_use_name_email_as_proof(store):
    t=store.metadata.tables['contact']
    with store.engine.begin() as conn:conn.execute(t.insert().values(id=uuid.uuid4(),type='person',name='Gabriel Example',email='shared@example.test'))
    with pytest.raises(ServiceError,match='identity review'):reconcile(store,create_account(store))
    assert len(rows(store,'contact'))==1 and rows(store,'account_contact')==[]


def test_legacy_identity_is_taken_only_from_independently_id_linked_holder(store):
    prior=create_account(store);t=store.metadata.tables['contact'];l=store.metadata.tables['account_contact'];contact_id=uuid.uuid4()
    with store.engine.begin() as conn:
        conn.execute(t.insert().values(id=contact_id,type='person',name='Gabriel Example',email='shared@example.test'))
        conn.execute(l.insert().values(id=uuid.uuid4(),account_id=uuid.UUID(prior),contact_id=contact_id,external_id='holder-one'))
    linked=reconcile(store,create_account(store))
    assert linked[0]['contact_id']==str(contact_id) and len(rows(store,'contact'))==1
    assert len(rows(store,'contact_identity'))==1


def test_name_based_legacy_holder_resolution_never_supplies_identity_proof(store):
    prior=create_account(store);t=store.metadata.tables['contact'];l=store.metadata.tables['account_contact'];contact_id=uuid.uuid4()
    with store.engine.begin() as conn:
        conn.execute(t.insert().values(id=contact_id,type='person',name='Gabriel Example',email='shared@example.test'))
        conn.execute(l.insert().values(id=uuid.uuid4(),account_id=uuid.UUID(prior),contact_id=contact_id))
    with pytest.raises(ServiceError,match='identity review'):reconcile(store,create_account(store))
    assert rows(store,'contact_identity')==[]


def test_concurrent_requests_across_applications_converge_on_one_identity(store):
    ids=[create_account(store) for _ in range(4)]
    with ThreadPoolExecutor(4) as pool:result=list(pool.map(lambda id:reconcile(store,id),ids*2))
    assert len({r[0]['contact_id'] for r in result})==1
    assert len(rows(store,'contact'))==len(rows(store,'contact_identity'))==len(rows(store,'contact_identity_document'))==1
    assert len(rows(store,'account_contact'))==4


def test_private_table_roles_have_no_access_and_rls_enabled(store):
    with store.engine.connect() as conn:
        for table in ('contact_identity','contact_identity_document'):
            assert conn.execute(text("select relrowsecurity from pg_class where oid=cast(:table as regclass)"),{'table':table}).scalar()
            for role in ('anon','authenticated'):
                assert not conn.execute(text("select has_table_privilege(:role,:table,'SELECT,INSERT,UPDATE,DELETE')"),{'role':role,'table':table}).scalar()


def test_endpoint_rejects_identity_override_missing_account_and_live_account(store,monkeypatch):
    id=create_account(store);http,headers,_=http_client(monkeypatch,store)
    assert http.post('/contacts/reconcile_application',headers=headers,json={'account_id':id,'customer':{}}).status_code==422
    assert http.post('/contacts/reconcile_application',headers=headers,json={'account_id':str(uuid.uuid4())}).status_code==404
    with store.engine.begin() as conn:
        t=store.metadata.tables['account'];conn.execute(t.update().where(t.c.id==uuid.UUID(id)).values(ibkr_account_number='U-FIXTURE'))
    assert http.post('/contacts/reconcile_application',headers=headers,json={'account_id':id}).status_code==409
    assert rows(store,'contact')==[]


def test_same_link_cannot_silently_replace_an_existing_id(store):
    id=create_account(store);reconcile(store,id)
    replace_payload(store,id,application(holder(number='98765')))
    with pytest.raises(ServiceError,match='Identification changed'):reconcile(store,id)
    assert rows(store,'contact_identity_document')[0]['identifier']=='001234'


def test_additional_document_is_retained_when_existing_id_corroborates_person(store):
    id=create_account(store);reconcile(store,id)
    person=holder();person['identification']['passport']='P-00002'
    replace_payload(store,id,application(person));reconcile(store,id)
    assert len(rows(store,'contact_identity_document'))==2
    assert {r['identifier'] for r in rows(store,'contact_identity_document')}=={'001234','P00002'}


def test_conflicting_second_holder_rolls_back_first_holder_link(store):
    reconcile(store,create_account(store))
    joint={'customer':{'type':'JOINT','jointHolders':{'firstHolderDetails':[holder(first='Luis',number='98765')],
           'secondHolderDetails':[holder(birth='1992-02-02',external='second')]}}}
    id=create_account(store,joint)
    with pytest.raises(ServiceError):reconcile(store,id)
    assert len(rows(store,'contact'))==1 and len(rows(store,'account_contact'))==1


@pytest.mark.parametrize('value', ['bad',0,None,'2035-01-01'])
def test_missing_invalid_future_birthday_never_creates_contacts(store,value):
    with pytest.raises(ServiceError):reconcile(store,create_account(store,application(holder(birth=value))))
    assert rows(store,'contact')==[]


def test_legacy_birthday_conflict_is_not_ignored(store):
    prior=create_account(store,application(holder(birth='1992-02-02')))
    t=store.metadata.tables['contact'];l=store.metadata.tables['account_contact'];id=uuid.uuid4()
    with store.engine.begin() as conn:
        conn.execute(t.insert().values(id=id,type='person',name='Gabriel Example',email='shared@example.test'))
        conn.execute(l.insert().values(id=uuid.uuid4(),account_id=uuid.UUID(prior),contact_id=id,external_id='holder-one'))
    with pytest.raises(ServiceError,match='birthday disagree'):reconcile(store,create_account(store))
    assert rows(store,'contact_identity')==[] and len(rows(store,'account_contact'))==1


def test_multiple_existing_legacy_contacts_with_same_id_require_review(store):
    for _ in range(2):
        prior=create_account(store);id=uuid.uuid4()
        with store.engine.begin() as conn:
            conn.execute(store.metadata.tables['contact'].insert().values(id=id,type='person',name='Gabriel Example',email='shared@example.test'))
            conn.execute(store.metadata.tables['account_contact'].insert().values(id=uuid.uuid4(),account_id=uuid.UUID(prior),contact_id=id,external_id='holder-one'))
    with pytest.raises(ServiceError,match='More than one'):reconcile(store,create_account(store))
    assert rows(store,'contact_identity')==[]


def test_company_link_cannot_claim_a_person_identity(store):
    id=create_account(store);company=uuid.uuid4()
    with store.engine.begin() as conn:
        conn.execute(store.metadata.tables['contact'].insert().values(id=company,type='company',name='Gabriel Example'))
        conn.execute(store.metadata.tables['account_contact'].insert().values(id=uuid.uuid4(),account_id=uuid.UUID(id),contact_id=company,external_id='holder-one'))
    with pytest.raises(ServiceError,match='Company and person'):reconcile(store,id)
    assert rows(store,'contact_identity')==[]


def test_generic_contact_response_does_not_include_private_identity(store,monkeypatch):
    reconcile(store,create_account(store));http,headers,_=http_client(monkeypatch,store)
    from src.components.clients import contacts
    store.read=lambda table,query: [dict(r) for r in rows(store,table)]
    monkeypatch.setattr(contacts,'db',store)
    response=http.get('/contacts/read',headers=headers)
    assert response.status_code==200
    assert 'date_of_birth' not in response.text and 'identifier' not in response.text


def test_new_identity_tables_match_declared_preflight_models(store):
    from sqlalchemy import inspect
    from sqlalchemy.orm import declarative_base
    from src.utils.connectors.supabase import Supabase
    from src.utils.managers.database_manager import DatabaseManager
    models=object.__new__(Supabase);models.Base=declarative_base();models._setup_models()
    manager=DatabaseManager(models.Base,store.engine)
    for table in ('contact_identity','contact_identity_document'):
        manager._validate_table_schema(inspect(store.engine),table)


def test_existing_stable_link_cannot_override_independent_legacy_birthday(store):
    prior=create_account(store,application(holder(birth='1992-02-02')));current=create_account(store);id=uuid.uuid4()
    with store.engine.begin() as conn:
        conn.execute(store.metadata.tables['contact'].insert().values(id=id,type='person',name='Gabriel Example',email='shared@example.test'))
        for account in (prior,current):
            conn.execute(store.metadata.tables['account_contact'].insert().values(id=uuid.uuid4(),account_id=uuid.UUID(account),contact_id=id,external_id='holder-one'))
    with pytest.raises(ServiceError,match='birthdays disagree'):reconcile(store,current)
    assert rows(store,'contact_identity')==[]


def test_contact_scope_alone_cannot_write_account_links(store,monkeypatch):
    id=create_account(store);http,headers,_=http_client(monkeypatch,store)
    monkeypatch.setattr(authz,'_load_user',lambda identity:{'id':identity,'scopes':'contacts'})
    assert http.post('/contacts/reconcile_application',headers=headers,json={'account_id':id}).status_code==403
    assert rows(store,'contact')==[]
    monkeypatch.setattr(authz,'_load_user',lambda identity:{'id':identity,'scopes':'contacts/reconcile_application accounts/contact'})
    assert http.post('/contacts/reconcile_application',headers=headers,json={'account_id':id}).status_code==200


def test_trusted_legacy_link_fills_missing_contact_type_without_retyping_known_company(store):
    id=create_account(store);contact_id=uuid.uuid4()
    with store.engine.begin() as conn:
        conn.execute(store.metadata.tables['contact'].insert().values(id=contact_id,name='Gabriel Example',email='shared@example.test'))
        conn.execute(store.metadata.tables['account_contact'].insert().values(id=uuid.uuid4(),account_id=uuid.UUID(id),contact_id=contact_id,external_id='holder-one'))
    result=reconcile(store,id)
    assert result[0]['contact_id']==str(contact_id)
    assert rows(store,'contact')[0]['type']=='person'
