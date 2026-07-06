from flask import Blueprint, request
from src.utils.response import format_response
from src.components.clients.documents import read_documents, get_document_data

bp = Blueprint('documents', __name__)

@bp.route('/read', methods=['GET'])
@format_response
def read_documents_route():
    """Read all document records and contact-document links without returning raw file data."""
    include_processing = request.args.get('include_processing', 'false').strip().lower() in ('1', 'true', 'yes')
    documents, contact_documents = read_documents(strip_data=True, include_processing=include_processing)
    return {'documents': documents, 'contact_documents': contact_documents }

@bp.route('/get_document_data', methods=['GET'])
@format_response
def get_document_data_route():
    """Read the raw stored data for a single document by document_id."""
    document_id = request.args.get('document_id', None)
    return get_document_data(document_id=document_id)
