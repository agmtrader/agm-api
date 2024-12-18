from flask import request, Blueprint
from src.components.database import Firebase

bp = Blueprint('database', __name__)

Database = Firebase()

@bp.route('/clear_collection', methods=['POST'])
def clear_collection_route():
    body = request.get_json(force=True)
    path = body['path']
    return Database.clear_collection(path)

@bp.route('/upload_collection', methods=['POST'])
def upload_collection_route():
    body = request.get_json(force=True)
    path = body['path']
    data = body['data']
    return Database.upload_collection(path, data)

@bp.route('/list_subcollections', methods=['POST'])
def list_subcollections_route():
    payload = request.get_json(force=True)
    path = payload['path']
    return Database.listSubcollections(path)

@bp.route('/create', methods=['POST'])
def create_route():
    body = request.get_json(force=True)
    data = body['data']
    path = body['path']
    id = body['id']
    return Database.create(data, path, id)

@bp.route('/read', methods=['POST'])
def read_route():
    body = request.get_json(force=True)
    path = body['path']
    query = body.get('query')
    return Database.read(path, query)

@bp.route('/update', methods=['POST'])
def update_route():
    body = request.get_json(force=True)
    path = body['path']
    data = body['data']
    query = body['query']
    return Database.update(path, data, query)

@bp.route('/delete', methods=['POST'])
def delete_route():
    body = request.get_json(force=True)
    path = body['path']
    return Database.delete(path)