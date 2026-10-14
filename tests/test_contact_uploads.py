"""Upload contract through actual Flask/contacts/OCR code with synthetic persisted files."""
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
import types
import uuid
from unittest.mock import Mock

import pytest
from flask import Flask, g
from flask_jwt_extended import JWTManager, create_access_token
from sqlalchemy import Column, ForeignKey, MetaData, Table, Text, Uuid, create_engine, event, select
from sqlalchemy.engine import make_url

from src.components.clients import contact_uploads, contacts, document_processing
from src.app.clients.contacts import bp
from src.utils import authz
from src.utils.managers.database_manager import DatabaseManager


@pytest.fixture
def upload_store(tmp_path, monkeypatch):
    metadata = MetaData()
    for name, extra in {
        'document': ['file_name','file_length','sha1_checksum','mime_type','data'],
        'contact_document': ['category','type','document_language','issued_date','expiry_date','comment'],
        'document_processing': ['process_type','status','source_language','output_text','provider','error'],
    }.items():
        columns = [Column('id', Uuid, primary_key=True), Column('created', Text), Column('updated', Text)]
        if name != 'document': columns.append(Column('document_id', Uuid, ForeignKey('document.id', ondelete='CASCADE' if name == 'document_processing' else 'SET NULL'), nullable=False))
        if name == 'contact_document': columns += [Column('contact_id', Uuid), Column('account_id', Uuid)]
        Table(name, metadata, *columns, *[Column(c, Text) for c in extra])
    postgres_url = os.getenv('AGM_UPLOAD_TEST_DATABASE_URL')
    locks = []
    if postgres_url:
        url = make_url(postgres_url)
        assert url.host in {'localhost', '127.0.0.1'} and url.database == 'agm_upload_fixture'
        engine = create_engine(url)
    else:
        engine = create_engine('sqlite:///' + str(tmp_path/'synthetic.db'))
        @event.listens_for(engine, 'connect')
        def connect(dbapi, _):
            # SQLite fixture emulates serialization; CI uses real PostgreSQL locks.
            dbapi.isolation_level = None
            dbapi.execute("PRAGMA foreign_keys=ON")
            dbapi.create_function('pg_advisory_xact_lock', 1, lambda key: locks.append(key) or 0)
        @event.listens_for(engine, 'begin')
        def begin(connection): connection.exec_driver_sql('BEGIN IMMEDIATE')
    metadata.create_all(engine)
    store = types.SimpleNamespace(engine=engine, metadata=metadata)
    store.get_schema = lambda table: {c.name: {} for c in metadata.tables[table].columns}
    store._dates_to_timestamp = lambda data: DatabaseManager._dates_to_timestamp(None, data)
    def conditions(table, query):
        return [table.c[k] == (uuid.UUID(str(v)) if isinstance(table.c[k].type, Uuid) and v else v)
                for k,v in query.items()]
    def read(table, query):
        tbl=metadata.tables[table]
        with engine.begin() as conn:
            rows=conn.execute(select(tbl).where(*conditions(tbl,query))).mappings().all()
            return [{k:str(v) if isinstance(v,uuid.UUID) else v for k,v in row.items()} for row in rows]
    def update(table, query, data):
        tbl=metadata.tables[table]
        with engine.begin() as conn: conn.execute(tbl.update().where(*conditions(tbl,query)).values(**{k:uuid.UUID(str(v)) if isinstance(tbl.c[k].type, Uuid) and v else v for k,v in data.items()}))
        return 'updated'
    store.read=read;store.update=update;store.locks=locks
    monkeypatch.setattr(contacts,'db',store)
    monkeypatch.setattr(document_processing,'_get_db',lambda:store)
    quality=types.SimpleNamespace(as_dict=lambda:{'status':'good'},status='good',score=90,
                                 average_confidence=.95)
    ocr=Mock(return_value=types.SimpleNamespace(text='Synthetic extracted content',provider=contact_uploads.GOOGLE_OCR_PROVIDER,
             quality=quality,pages=[],model_version='synthetic',pipeline_version='synthetic',device='google'))
    monkeypatch.setattr(document_processing,'extract_document_ocr',ocr)
    yield store,ocr
    metadata.drop_all(engine)
    engine.dispose()


def payload(contact_id=None, raw=b'synthetic document bytes'):
    return {'contact_id':str(contact_id or uuid.uuid4()),'file_name':'fixture.pdf','file_length':len(raw),
            'mime_type':'application/pdf','data':base64.b64encode(raw).decode(),
            'sha1_checksum':hashlib.sha1(raw).hexdigest(),'document_language':'es',
            'category':'Proof of Identity','type':'Passport','issued_date':'2026-10-14T00:00:00.000Z'}


def http_client(monkeypatch):
    app=Flask(__name__);app.config['JWT_SECRET_KEY']='synthetic-test-signing-key-at-least-32-bytes';JWTManager(app)
    monkeypatch.setattr(authz,'_load_user',lambda identity:{'id':identity,'scopes':'contacts' if identity=='allowed' else 'accounts/read'})
    app.before_request(lambda:setattr(g,'request_id','synthetic-upload'))
    app.before_request(authz.authenticate_request);app.before_request(authz.enforce_route_scope)
    app.register_blueprint(bp,url_prefix='/contacts')
    with app.app_context(): token=create_access_token(identity='allowed');denied=create_access_token(identity='denied')
    return app.test_client(),{'Authorization':'Bearer '+token},{'Authorization':'Bearer '+denied}


def test_upload_commits_file_link_processing_and_uses_google(upload_store, monkeypatch):
    store,ocr=upload_store;http,headers,_=http_client(monkeypatch);body=payload()
    def extract(*args,**kwargs):
        assert len(store.read('document',{}))==1
        assert len(store.read('contact_document',{}))==1
        assert store.read('document_processing',{})[0]['status']=='pending'
        assert kwargs['provider']==contact_uploads.GOOGLE_OCR_PROVIDER
        return ocr.return_value
    ocr.side_effect=extract
    response=http.post('/contacts/documents',headers=headers,json=body)
    assert response.status_code==200
    assert set(response.json)=={'id','document_id','duplicate','link_reused'}
    assert response.json['duplicate'] is False
    assert store.read('document',{})[0]['sha1_checksum']==hashlib.sha1(b'synthetic document bytes').hexdigest()
    assert store.read('contact_document',{})[0]['issued_date']=='20261014000000'
    processing=store.read('document_processing',{})[0]
    assert processing['status']=='completed' and processing['output_text']=='Synthetic extracted content'
    assert processing['provider']==contact_uploads.GOOGLE_OCR_PROVIDER


@pytest.mark.parametrize('failure', [TimeoutError('provider failed'),ValueError('missing config')])
def test_ocr_failure_still_returns_upload_success_and_is_retained(upload_store,monkeypatch,failure):
    store,ocr=upload_store;ocr.side_effect=failure;http,headers,_=http_client(monkeypatch);body=payload()
    first=http.post('/contacts/documents',headers=headers,json=body)
    again=http.post('/contacts/documents',headers=headers,json=body)
    assert first.status_code==again.status_code==200
    assert again.json['document_id']==first.json['document_id'] and again.json['id']==first.json['id']
    assert again.json['duplicate'] is True
    assert set(first.json)=={'id','document_id','duplicate','link_reused'}
    assert len(store.read('document',{}))==len(store.read('contact_document',{}))==len(store.read('document_processing',{}))==1
    assert store.read('document_processing',{})[0]['status']=='failed'
    assert ocr.call_count==1


def test_repeated_and_cross_contact_uploads_reuse_completed_extraction(upload_store):
    store,ocr=upload_store;body=payload();first=contacts.upload_contact_document(**body)
    same=contacts.upload_contact_document(**body)
    other=contacts.upload_contact_document(**{**body,'contact_id':str(uuid.uuid4())})
    assert first['id']==same['id'] and other['id']!=same['id']
    assert first['document_id']==same['document_id']==other['document_id']
    assert len(store.read('document',{}))==len(store.read('document_processing',{}))==1
    assert len(store.read('contact_document',{}))==2 and ocr.call_count==1


def test_concurrent_identical_uploads_claim_only_one_attempt(upload_store):
    store,ocr=upload_store;body=payload()
    with ThreadPoolExecutor(6) as pool:results=list(pool.map(lambda _:contacts.upload_contact_document(**body),range(6)))
    assert len({r['document_id'] for r in results})==len({r['id'] for r in results})==1
    assert sum(not r['duplicate'] for r in results)==1
    assert ocr.call_count==1
    assert len(store.read('document',{}))==len(store.read('contact_document',{}))==len(store.read('document_processing',{}))==1
    if store.engine.dialect.name == "sqlite": assert len(set(store.locks))==1


@pytest.mark.parametrize('change',[{'data':'invalid'}, {'data':''}, {'sha1_checksum':'0'*40}, {'contact_id':'bad'}])
def test_invalid_input_creates_no_rows_or_provider_calls(upload_store,monkeypatch,change):
    store,ocr=upload_store;http,headers,_=http_client(monkeypatch)
    response=http.post('/contacts/documents',headers=headers,json={**payload(),**change})
    assert response.status_code==422
    assert all(not store.read(t,{}) for t in store.metadata.tables)
    ocr.assert_not_called()


def test_missing_scope_or_identity_cannot_upload_or_reuse(upload_store,monkeypatch):
    store,ocr=upload_store;http,headers,denied=http_client(monkeypatch);body=payload()
    assert http.post('/contacts/documents',headers=headers,json=body).status_code==200
    assert http.post('/contacts/documents',headers=denied,json=body).status_code==403
    assert http.post('/contacts/documents',json=body).status_code==401
    assert len(store.read('contact_document',{}))==1 and ocr.call_count==1


def test_processing_persistence_outage_preserves_file_success_and_pending_recovery(upload_store,monkeypatch):
    store,ocr=upload_store;store.update=Mock(side_effect=RuntimeError('database outage'))
    http,headers,_=http_client(monkeypatch)
    response=http.post('/contacts/documents',headers=headers,json=payload())
    assert response.status_code==200
    assert len(store.read('document',{}))==len(store.read('contact_document',{}))==1
    assert store.read('document_processing',{})[0]['status']=='pending'


def test_link_insert_failure_rolls_back_file_and_processing(upload_store,monkeypatch):
    store,ocr=upload_store
    @event.listens_for(store.engine,'before_cursor_execute')
    def fail_link(conn,cursor,statement,parameters,context,many):
        if statement.startswith('INSERT INTO contact_document'):raise RuntimeError('synthetic link write failure')
    http,headers,_=http_client(monkeypatch)
    assert http.post('/contacts/documents',headers=headers,json=payload()).status_code==500
    assert all(not store.read(t,{}) for t in store.metadata.tables)
    ocr.assert_not_called()



def test_edit_and_unlink_preserve_other_contact_and_raw_document(upload_store,monkeypatch):
    store,ocr=upload_store;http,headers,_=http_client(monkeypatch);body=payload()
    first=http.post('/contacts/documents',headers=headers,json=body).json
    other=http.post('/contacts/documents',headers=headers,json={**body,'contact_id':str(uuid.uuid4())}).json
    patch={'document_id':first['document_id'],'contact_document_id':first['id'],
           'category':'Proof of Address','issued_date':None}
    assert http.patch('/contacts/documents',headers=headers,json=patch).status_code==200
    rows={row['id']:row for row in store.read('contact_document',{})}
    assert rows[first['id']]['category']=='Proof of Address'
    assert rows[other['id']]['category']=='Proof of Identity'
    assert http.delete('/contacts/documents',headers=headers,json=patch).status_code==200
    assert len(store.read('document',{}))==len(store.read('contact_document',{}))==1
    assert len(store.read('document_processing',{}))==1
    assert store.read('document_processing',{})[0]['document_id']==other['document_id']
    assert store.read('contact_document',{})[0]['id']==other['id']
    assert http.delete('/contacts/documents',headers=headers,json={
        'document_id':other['document_id'],'contact_document_id':other['id']}).status_code==200
    assert not store.read('document',{}) and not store.read('contact_document',{})
    assert not store.read('document_processing',{})


@pytest.mark.parametrize('method',['patch','delete'])
def test_legacy_shared_document_mutation_is_rejected(upload_store,monkeypatch,method):
    store,ocr=upload_store;body=payload();first=contacts.upload_contact_document(**body)
    contacts.upload_contact_document(**{**body,'contact_id':str(uuid.uuid4())})
    http,headers,_=http_client(monkeypatch)
    response=getattr(http,method)('/contacts/documents',headers=headers,json={'document_id':first['document_id']})
    assert response.status_code==409
    assert len(store.read('document',{}))==1 and len(store.read('contact_document',{}))==2


def test_matching_hash_with_different_stored_bytes_is_not_reused(upload_store,monkeypatch):
    store,ocr=upload_store;body=payload();first=contacts.upload_contact_document(**body)
    store.update('document',{'id':first['document_id']},{'data':base64.b64encode(b'different').decode()})
    http,headers,_=http_client(monkeypatch)
    assert http.post('/contacts/documents',headers=headers,json={**body,'contact_id':str(uuid.uuid4())}).status_code==409
    assert len(store.read('contact_document',{}))==1 and ocr.call_count==1


def test_unsupported_ocr_file_remains_saved_successfully(upload_store,monkeypatch):
    store,ocr=upload_store;http,headers,_=http_client(monkeypatch)
    response=http.post('/contacts/documents',headers=headers,json={**payload(),'mime_type':'application/octet-stream'})
    assert response.status_code==200
    assert store.read('document_processing',{})[0]['status']=='failed'
    ocr.assert_not_called()


def test_processing_import_or_initialization_failure_is_internal(upload_store,monkeypatch):
    store,ocr=upload_store
    monkeypatch.setattr(document_processing,'process_document_text_extraction',Mock(side_effect=ImportError('missing library')))
    http,headers,_=http_client(monkeypatch)
    response=http.post('/contacts/documents',headers=headers,json=payload())
    assert response.status_code==200
    assert store.read('document_processing',{})[0]['status']=='failed'
    assert 'error' not in response.json


@pytest.mark.parametrize('original_account,next_account', [(None,uuid.uuid4()),(uuid.uuid4(),None),(uuid.uuid4(),uuid.uuid4())])
def test_same_contact_file_does_not_relink_across_account_contexts(upload_store,original_account,next_account):
    store,ocr=upload_store;body={**payload(),'account_id':str(original_account) if original_account else None}
    first=contacts.upload_contact_document(**body)
    second=contacts.upload_contact_document(**{**body,'account_id':str(next_account) if next_account else None,
                                              'category':'Other','comment':'Do not replace existing metadata'})
    assert second['id']==first['id'] and second['document_id']==first['document_id']
    assert second['link_reused'] is True
    assert len(store.read('contact_document',{}))==1
    row=store.read('contact_document',{})[0]
    assert row['account_id']==body['account_id'] and row['category']=='Proof of Identity'
    assert ocr.call_count==1
