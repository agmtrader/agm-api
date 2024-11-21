from flask import request, Blueprint
from src.components.database import Firebase
from src.utils.logger import logger

bp = Blueprint('database', __name__)

Database = Firebase()

@bp.route('/clear_collection', methods=['POST'])
def clear_collection_route():
    body = request.get_json(force=True)
    path = body['path']
    response = Database.clear_collection(path)
    return response

@bp.route('/upload_collection', methods=['POST'])
def upload_collection_route():
    body = request.get_json(force=True)
    path = body['path']
    data = body['data']
    response = Database.upload_collection(path, data)
    return response

@bp.route('/list_subcollections', methods=['POST'])
def list_subcollections_route():
    payload = request.get_json(force=True)
    path = payload['path']
    response = Database.listSubcollections(path)
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
    query = body.get('query')
    response = Database.read(path, query)
    logger.info(response)
    return response

@bp.route('/update', methods=['POST'])
def update_route():
    body = request.get_json(force=True)
    path = body['path']
    data = body['data']
    query = body['query']
    response = Database.update(path, data, query)
    return response

@bp.route('/delete', methods=['POST'])
def delete_route():
    body = request.get_json(force=True)
    path = body['path']
    response = Database.delete(path)
    return response