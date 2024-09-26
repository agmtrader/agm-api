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
    key = None
    value = None

    if 'key' in list(body.keys()):
        key = body['key']
    if 'value' in list(body.keys()):
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