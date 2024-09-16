from flask import request, Blueprint
from app.helpers.google import Firebase

bp = Blueprint('database', __name__)

# Database
@bp.route('/database/get_documents_in_collection', methods=['POST'])
def get_documents_in_collection_route():
    body = request.get_json(force=True)
    path = body['path']
    response = Firebase.getDocumentsFromCollection(path)
    return response

@bp.route('/database/query_documents_in_collection', methods=['POST'])
def query_documents_in_collection_route():
    body = request.get_json(force=True)
    path = body['path']
    key = body['key']
    value = body['value']
    response = Firebase.queryDocumentsInCollection(path, key, value)
    return response