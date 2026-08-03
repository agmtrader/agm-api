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

bp = Blueprint('contacts', __name__)

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
    """Read contacts from the database filtered by id or email address."""
    query = {}
    id = request.args.get('id', None)
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
    )


@bp.route('/documents', methods=['POST'])
@format_response
def upload_contact_document_route():
    """Upload and attach a document to a contact, including file metadata and optional review fields."""
    payload = request.get_json(force=True)
    return upload_contact_document(
        account_id=payload.get('account_id'),
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
    return delete_contact_document(document_id=payload.get('document_id'))


@bp.route('/screening', methods=['GET'])
@format_response
def read_contact_screenings_route():
    """Read screening results associated with a contact."""
    contact_id = request.args.get('contact_id', None)
    return read_contact_screenings(contact_id=contact_id)


@bp.route('/screening', methods=['POST'])
@format_response
def create_contact_screening_route():
    """Run or create a screening record for a contact using its contact_id."""
    payload = request.get_json(force=True)
    return create_contact_screening_from_contact_id(contact_id=payload.get('contact_id'))
