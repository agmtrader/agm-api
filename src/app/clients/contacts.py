from flask import Blueprint, request
from src.components.clients.contacts import (
    read_contacts,
    create_contact,
    update_contact,
    upload_contact_document,
    read_contact_documents,
    update_contact_document,
    delete_contact_document,
    create_contact_screening_from_contact_id,
    read_contact_screenings
)
from src.utils.response import format_response
from src.utils.exception import ServiceError
from src.utils.authz import require_scope

bp = Blueprint('contacts', __name__)

def _read_id_filter(name, values):
    """Accept one ID or up to 100 repeated IDs; blank filters never read all rows."""
    if not values:
        return None
    if len(values) > 100 or any(not value.strip() for value in values):
        raise ServiceError(f'{name} requires 1 to 100 nonblank IDs', status_code=400)
    ids = list(dict.fromkeys(value.strip() for value in values))
    return ids[0] if len(ids) == 1 else ids


@bp.route('/reconcile_application', methods=['POST'])
@require_scope('accounts/contact')
@format_response
def reconcile_application_route():
    """Resolve contacts from the saved pending application; never return raw identity."""
    from src.components.clients.contact_identity import reconcile_application_contacts
    from src.utils.connectors.supabase import db
    payload = request.get_json(force=True) or {}
    if not isinstance(payload, dict) or set(payload) != {'account_id'}:
        raise ServiceError('Only account_id is accepted for application contact setup.', 422, 'contact_identity_invalid')
    return reconcile_application_contacts(db, payload.get('account_id'))


@bp.route('/create', methods=['POST'])
@format_response
def create_contact_route():
    """Create a contact record."""
    payload = request.get_json(force=True)  
    contact = payload.get('contact', None)
    return create_contact(contact)

@bp.route('/read', methods=['GET'])
@format_response
def contacts_route():
    """Read contacts by id (repeat up to 100 for bulk reads) and/or email."""
    query = {}
    id = _read_id_filter('id', request.args.getlist('id'))
    email = request.args.get('email', None)
    if id:
        query['id'] = id
    if email:
        query['email'] = email
    return read_contacts(query=query)

@bp.route('/update', methods=['POST'])
@format_response
def update_contact_route():
    """Update contact records selected by the provided query payload."""
    payload = request.get_json(force=True)  
    contact = payload.get('contact', None)
    query = payload.get('query', None)
    return update_contact(query=query, contact=contact)


@bp.route('/documents', methods=['GET'])
@format_response
def read_contact_documents_route():
    """Read documents linked to a contact, with optional file data and document relationship metadata."""
    contact_id = request.args.get('contact_id', None)
    document_ids = request.args.getlist('document_id')
    include_data = request.args.get('include_data', 'false').strip().lower() in ('1', 'true', 'yes')
    include_documents = request.args.get('include_documents', 'true').strip().lower() in ('1', 'true', 'yes')
    include_processing = request.args.get('include_processing', 'false').strip().lower() in ('1', 'true', 'yes')
    return read_contact_documents(
        contact_id=contact_id,
        document_ids=document_ids,
        include_data=include_data,
        include_documents=include_documents,
        include_processing=include_processing,
        category=request.args.get('category'),
    )


@bp.route('/documents', methods=['POST'])
@format_response
def upload_contact_document_route():
    """Upload and attach a document to a contact, including file metadata and optional review fields."""
    payload = request.get_json(force=True)
    return upload_contact_document(
        account_id=payload.get('account_id') or None,
        contact_id=payload.get('contact_id'),
        file_name=payload.get('file_name'),
        file_length=payload.get('file_length'),
        sha1_checksum=payload.get('sha1_checksum'),
        mime_type=payload.get('mime_type'),
        data=payload.get('data'),
        category=payload.get('category'),
        type=payload.get('type'),
        document_language=payload.get('document_language'),
        issued_date=payload.get('issued_date'),
        expiry_date=payload.get('expiry_date'),
        comment=payload.get('comment')
    )


@bp.route('/documents', methods=['PATCH'])
@format_response
def update_contact_document_route():
    """Update metadata fields for a previously uploaded contact document."""
    payload = request.get_json(force=True)
    return update_contact_document(
        document_id=payload.get('document_id'),
        contact_document_id=payload.get('contact_document_id'),
        category=payload.get('category'),
        type=payload.get('type'),
        document_language=payload.get('document_language'),
        issued_date=payload.get('issued_date'),
        expiry_date=payload.get('expiry_date'),
        comment=payload.get('comment')
    )


@bp.route('/documents', methods=['DELETE'])
@format_response
def delete_contact_document_route():
    """Delete a contact document by document_id."""
    payload = request.get_json(force=True)
    return delete_contact_document(document_id=payload.get('document_id'),
                                   contact_document_id=payload.get('contact_document_id'))


@bp.route('/screening', methods=['GET'])
@format_response
def read_contact_screenings_route():
    """Read screening results by contact_id (repeat up to 100 for bulk reads)."""
    contact_id = _read_id_filter('contact_id', request.args.getlist('contact_id'))
    return read_contact_screenings(contact_id=contact_id, view=request.args.get('view', 'full'))


@bp.route('/screen', methods=['POST'])
@format_response
def create_contact_screening_route():
    """Run or create a screening record for one contact."""
    payload = request.get_json(force=True)
    lists = payload.get('lists') or {}
    if not isinstance(lists, dict):
        lists = {}
    ofac_list = lists.get('ofac') or payload.get('ofac_list')
    uk_list = lists.get('uk') or payload.get('uk_list')
    un_list = lists.get('un') or payload.get('un_list')
    sanctions_lists = (ofac_list, uk_list, un_list) if all(isinstance(value, list) for value in (ofac_list, uk_list, un_list)) else None
    return create_contact_screening_from_contact_id(
        contact_id=payload.get('contact_id'),
        sanctions_lists=sanctions_lists,
    )
