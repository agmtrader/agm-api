from flask import request, Blueprint
from app.helpers.google import Firebase

bp = Blueprint('database', __name__)

Database = Firebase()

@bp.route('/clear_collection', methods=['POST'])
def clear_collection_route():
    body = request.get_json(force=True)
    path = body['path']
    response = Database.clearCollection(path)
    return response

@bp.route('/upload_collection', methods=['POST'])
def upload_collection_route():
    body = request.get_json(force=True)
    path = body['path']
    data = body['data']
    response = Database.upload_collection(path, data)
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
    query = body.get('query')  # Use .get() to handle cases where 'query' is not provided
    response = Database.read(path, query)
    return response

@bp.route('/update', methods=['POST'])
def update_route():
    body = request.get_json(force=True)
    path = body['path']
    data = body['data']
    response = Database.update(path, data)
    return response

@bp.route('/delete', methods=['POST'])
def delete_route():
    body = request.get_json(force=True)
    path = body['path']
    response = Database.delete(path)
    return response

@bp.route('/list_subcollections', methods=['POST'])
def list_subcollections_route():
    payload = request.get_json(force=True)
    parent_id = payload['parent_id']
    response = Database.listSubcollections(parent_id)
    return response