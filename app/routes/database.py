from flask import request, Blueprint
from app.helpers.google import Firebase

bp = Blueprint('database', __name__)

Database = Firebase()

@bp.route('/get_documents_in_collection', methods=['POST'])
def get_documents_in_collection_route():
    body = request.get_json(force=True)
    path = body['path']
    response = Database.getDocumentsInCollection(path)
    return response

@bp.route('/add_dataframe_to_collection', methods=['POST'])
def add_dataframe_to_collection_route():
    body = request.get_json(force=True)
    df = body['df']
    path = body['path']
    response = Database.addDataframeToCollection(df, path)
    return response


@bp.route('/create', methods=['POST'])
def create_route():
    body = request.get_json(force=True)
    data = body['data']
    path = body['path']
    id = body['id']
    response = Database.create(data, path, id)
    return response

@bp.route('/read', methods=['POST'])
def read_route():
    body = request.get_json(force=True)
    path = body['path']
    key = body['key']
    value = body['value']
    response = Database.read(path, key, value)
    return response

@bp.route('/update', methods=['POST'])
def update_route():
    body = request.get_json(force=True)
    path = body['path']
    key = body['key']
    value = body['value']
    response = Database.update(path, key, value)
    return response

@bp.route('/delete', methods=['POST'])
def delete_route():
    body = request.get_json(force=True)
    path = body['path']
    response = Database.delete(path)
    return response