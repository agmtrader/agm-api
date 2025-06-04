from flask import Blueprint, request
from src.components.documents.client_documents import read, delete, upload_poa, upload_poi
from src.utils.managers.scope_manager import verify_scope
from src.utils.logger import logger
from src.utils.managers.scope_manager import enforce_user_filter

logger.announcement('Initializing Client Documents Service', type='info')
bp = Blueprint('client_documents', __name__)
logger.announcement('Initialized Client Documents Service', type='success')

@bp.route('/read', methods=['POST'])
@verify_scope('documents/read')
@enforce_user_filter()
def read_route():
  payload = request.get_json(force=True)
  query = payload.get('query', None)
  return read(query=query)

@bp.route('/delete', methods=['POST'])
@verify_scope('documents/delete')
def delete_route():
  payload = request.get_json(force=True)
  document = payload.get('document', None)
  bucket_id = payload.get('bucket_id', None)
  return delete(document=document, bucket_id=bucket_id)

@bp.route('/upload_poa', methods=['POST'])
@verify_scope('documents/upload')
def upload_poa_route():
  payload = request.get_json(force=True)

  f = {
    'file_name': payload.get('file_name', None),
    'mime_type': payload.get('mime_type', None),
    'file_data': payload.get('file_data', None),
  }

  document_info = payload.get('document_info', None)
  user_id = payload.get('user_id', None)

  return upload_poa(f, document_info=document_info, user_id=user_id)

@bp.route('/upload_poi', methods=['POST'])
@verify_scope('documents/upload')
def upload_poi_route():
  payload = request.get_json(force=True)

  f = {
    'file_name': payload.get('file_name', None),
    'mime_type': payload.get('mime_type', None),
    'file_data': payload.get('file_data', None),
  }

  document_info = payload.get('document_info', None)
  user_id = payload.get('user_id', None)

  return upload_poi(f, document_info=document_info, user_id=user_id)