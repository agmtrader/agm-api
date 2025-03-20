from flask import Blueprint, request
from src.components.document_center import DocumentCenter
from src.utils.scope_manager import verify_scope

bp = Blueprint('document_center', __name__)
DocumentCenter = DocumentCenter()

@bp.route('/get_folder_dictionary', methods=['GET'])
@verify_scope('document_center/get_folder_dictionary')
def get_folder_dictionary_route():
  return DocumentCenter.get_folder_dictionary()

@bp.route('/read', methods=['POST'])
@verify_scope('document_center/read')
def read_route():
  body = request.get_json(force=True)
  query = body['query']
  return DocumentCenter.read_files(query)

@bp.route('/delete', methods=['POST'])
@verify_scope('document_center/delete')
def delete_route():
  body = request.get_json(force=True)
  document = body['document']
  parent_folder_id = body['parent_folder_id']
  return DocumentCenter.delete_file(document, parent_folder_id)

@bp.route('/upload', methods=['POST'])
@verify_scope('document_center/upload')
def upload_route():
  body = request.get_json(force=True)
  file_name = body['file_name']
  mime_type = body['mime_type']
  file_data = body['file_data']
  parent_folder_id = body['parent_folder_id']
  document_info = body['document_info']
  uploader = body['uploader']
  return DocumentCenter.upload_file(file_name, mime_type, file_data, parent_folder_id, document_info, uploader)