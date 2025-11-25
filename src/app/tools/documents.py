from flask import Blueprint, request
from src.utils.response import format_response

from src.components.tools.documents import read_documents, get_document_data

bp = Blueprint('documents', __name__)

@bp.route('/read', methods=['GET'])
@format_response
def read_documents_route():
    account_id = request.args.get('account_id', None)
    documents, account_documents = read_documents(account_id=account_id, strip_data=True)
    return {'documents': documents, 'account_documents': account_documents }

@bp.route('/get_document_data', methods=['GET'])
@format_response
def get_document_data_route():
    document_id = request.args.get('document_id', None)
    return get_document_data(document_id=document_id)