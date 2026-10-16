from unittest.mock import Mock
from src.components.clients import contacts
from src.app.clients import risk_profiles
from flask import Flask


def test_ocr_reads_only_processing_for_linked_document_ids(monkeypatch):
    db=Mock()
    def read(table,query,**kwargs):
        if table=='contact_document':return [{'document_id':'doc-one','contact_id':'selected'}]
        if table=='document_processing':
            assert query=={'process_type':'text_extraction','document_id':['doc-one']}
            return [{'document_id':'doc-one','output_text':'synthetic'}]
        if table=='document':return [{'id':'doc-one'}]
        raise AssertionError(table)
    db.read.side_effect=read;monkeypatch.setattr(contacts,'db',db)
    for include_documents in [False,True]:
        result=contacts.read_contact_documents(contact_id='selected',include_documents=include_documents,include_processing=True)
        assert result['contact_documents'][0]['document_id']=='doc-one'
    assert db.read.call_count==5


def test_empty_links_do_not_load_the_processing_population(monkeypatch):
    db=Mock();db.read.return_value=[];monkeypatch.setattr(contacts,'db',db)
    assert contacts.read_contact_documents(contact_id='selected',include_processing=True)=={'documents':[],'contact_documents':[]}
    assert db.read.call_count==1


def test_contact_profile_route_filters_by_id_and_contact_without_global_fallback(monkeypatch):
    loader=Mock(return_value=[]);monkeypatch.setattr(risk_profiles,'read_risk_profiles',loader)
    app=Flask(__name__);app.register_blueprint(risk_profiles.bp,url_prefix='/risk_profiles')
    response=app.test_client().get('/risk_profiles/read?contact_id=selected&id=profile')
    assert response.status_code==200
    loader.assert_called_once_with(query={'contact_id':'selected','id':'profile'})


def test_bulk_contact_and_screening_routes_apply_one_filtered_database_read(monkeypatch):
    from src.app.clients import contacts as routes
    db = Mock()
    db.read.return_value = []
    monkeypatch.setattr(contacts, 'db', db)
    app = Flask(__name__)
    app.register_blueprint(routes.bp, url_prefix='/contacts')
    client = app.test_client()
    for url, table, query in [
        ('/contacts/read?id=one&id=two&id=one', 'contact', {'id': ['one', 'two']}),
        ('/contacts/read?id=one&email=own@example.com', 'contact', {'id': 'one', 'email': 'own@example.com'}),
        ('/contacts/read', 'contact', {}),
        ('/contacts/screening?contact_id=one&contact_id=two', 'contact_screening', {'contact_id': ['one', 'two']}),
        ('/contacts/screening?contact_id=one', 'contact_screening', {'contact_id': 'one'}),
        ('/contacts/screening', 'contact_screening', {}),
    ]:
        db.read.reset_mock()
        response = client.get(url)
        assert response.status_code == 200
        assert response.get_json() == []
        db.read.assert_called_once_with(table=table, query=query)


def test_bulk_id_filters_reject_blank_and_oversized_selections_without_reading(monkeypatch):
    from src.app.clients import contacts as routes
    db = Mock()
    monkeypatch.setattr(contacts, 'db', db)
    app = Flask(__name__)
    app.register_blueprint(routes.bp, url_prefix='/contacts')
    for path, parameter in [('/read', 'id'), ('/screening', 'contact_id')]:
        for values in [[''], ['one', ' '], ['one'] * 101]:
            response = app.test_client().get('/contacts' + path, query_string=[(parameter, value) for value in values])
            assert response.status_code == 400
    db.read.assert_not_called()
