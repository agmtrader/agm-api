from src.utils.exception import handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger
from typing import Optional

logger.announcement('Initializing Contacts Service', type='info')
logger.announcement('Initialized Contacts Service', type='success')

contact_table = 'contact'
contact_document_table = 'contact_document'
contact_screening_table = 'contact_screening'

def _filter_supported_columns(table: str, payload: Optional[dict]) -> dict:
    payload = payload or {}
    if not payload:
        return {}
    schema = db.get_schema(table)
    allowed_columns = set(schema.keys())
    return {key: value for key, value in payload.items() if key in allowed_columns}

@handle_exception
def create_contact(contact: dict = None):
    filtered_contact = _filter_supported_columns(contact_table, contact)
    contact_id = db.create(table=contact_table, data=filtered_contact)
    return {'id': contact_id}

@handle_exception
def read_contacts(query=None):
    contacts = db.read(table=contact_table, query=query)
    return contacts

@handle_exception
def update_contact(query: dict = None, contact: dict = None):
    filtered_contact = _filter_supported_columns(contact_table, contact)
    if len(filtered_contact) == 0:
        return {'id': str((query or {}).get('id') or ''), 'status': 'skipped'}
    contact_id = db.update(table=contact_table, query=query, data=filtered_contact)
    return {'id': contact_id}


@handle_exception
def upload_contact_document(
    account_id: str = None,
    contact_id: str = None,
    file_name: str = None,
    file_length: int = None,
    sha1_checksum: str = None,
    mime_type: str = None,
    data: str = None,
    category: str = None,
    type: str = None,
    issued_date: str = None,
    expiry_date: str = None,
    comment: Optional[str] = None
):
    if not contact_id:
        raise Exception('contact_id is required')
    if not account_id:
        raise Exception('account_id is required')

    document_id = db.create(
        table='document',
        data={
            'file_name': file_name,
            'file_length': file_length,
            'sha1_checksum': sha1_checksum,
            'mime_type': mime_type,
            'data': data
        }
    )

    link_id = db.create(
        table=contact_document_table,
        data={
            'account_id': account_id,
            'contact_id': contact_id,
            'document_id': document_id,
            'category': category,
            'type': type,
            'issued_date': issued_date,
            'expiry_date': expiry_date,
            'comment': comment
        }
    )
    return {'id': link_id, 'document_id': document_id}


@handle_exception
def read_contact_documents(contact_id: str = None, include_data: bool = False, include_documents: bool = True):
    query = {'contact_id': contact_id} if contact_id else {}
    links = db.read(table=contact_document_table, query=query) or []
    if not include_documents:
        return {'documents': [], 'contact_documents': links}

    documents = []
    exclude = None if include_data else ['data']
    for link in links:
        document = db.read(table='document', query={'id': link.get('document_id')}, exclude_columns=exclude) or []
        documents.extend(document)
    return {'documents': documents, 'contact_documents': links}


@handle_exception
def update_contact_document(
    document_id: str = None,
    category: str = None,
    type: str = None,
    issued_date: str = None,
    expiry_date: str = None,
    comment: Optional[str] = None
):
    return db.update(
        table=contact_document_table,
        query={'document_id': document_id},
        data={
            'category': category,
            'type': type,
            'issued_date': issued_date,
            'expiry_date': expiry_date,
            'comment': comment
        }
    )


@handle_exception
def delete_contact_document(document_id: str = None):
    db.delete(table=contact_document_table, query={'document_id': document_id})
    db.delete(table='document', query={'id': document_id})
    return {'status': 'success'}


@handle_exception
def create_contact_screening(
    contact_id: str = None,
    risk_score: Optional[float] = None,
    fatf_status: Optional[str] = None,
    un_status: Optional[str] = None,
    uk_status=None,
    ofac_results=None,
    created: Optional[str] = None
):
    if not contact_id:
        raise Exception('contact_id is required')
    return db.create(table=contact_screening_table, data={
        'contact_id': contact_id,
        'risk_score': risk_score if risk_score is not None else 0,
        'fatf_status': fatf_status or 'Not listed',
        'un_status': un_status,
        'uk_status': uk_status if uk_status is not None else [],
        'ofac_results': ofac_results if ofac_results is not None else [],
        'created': created
    })


@handle_exception
def read_contact_screenings(contact_id: str = None):
    if not contact_id:
        raise Exception('contact_id is required')
    return db.read(table=contact_screening_table, query={'contact_id': contact_id}) or []
