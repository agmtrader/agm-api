from flask import Blueprint, request, send_file
from app.helpers.google import GoogleDrive
from io import BytesIO
import base64
bp = Blueprint('drive', __name__)

Drive = GoogleDrive()

@bp.route('/get_shared_drive_info', methods=['POST'])
def get_shared_drive_info():
    payload = request.get_json(force=True)
    drive_name = payload['drive_name']
    response = Drive.getSharedDriveInfo(drive_name)
    return response

@bp.route('/get_folder_info', methods=['POST'])
def get_folder_info_route():
    payload = request.get_json(force=True)
    parent_id = payload['parent_id']
    folder_name = payload['folder_name']
    response = Drive.getFolderInfo(parent_id, folder_name)
    return response

@bp.route('/get_files_in_folder', methods=['POST'])
def get_files_in_folder_route():
    payload = request.get_json(force=True)
    parent_id = payload['parent_id']
    response = Drive.getFilesInFolder(parent_id)
    return response



@bp.route('/delete_files', methods=['POST'])
def delete_files_route():
    payload = request.get_json(force=True)
    file_ids = payload['file_ids']
    response = Drive.deleteFiles(file_ids)
    return response

@bp.route('/move_file', methods=['POST'])
def move_file_route():
    payload = request.get_json(force=True)
    f = payload['file']
    new_parent_id = payload['new_parent_id']
    response = Drive.moveFile(f, new_parent_id)
    return response

@bp.route('/download_file', methods=['POST'])
def download_file_route():
    payload = request.get_json(force=True)
    response = Drive.downloadFile(payload['file_id'])
    f = BytesIO(response['content'])
    return send_file(f, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@bp.route('/rename_files', methods=['POST'])
def rename_files_route():
    payload = request.get_json(force=True)
    files = payload['files']
    response = Drive.renameFiles(files)
    return response

@bp.route('/upload_file', methods=['POST'])
def upload_file_route():
    payload = request.get_json(force=True)
    raw_file = base64.b64decode(payload['raw_file'])
    file_name = payload['file_name']
    mime_type = payload['mime_type']
    parent_id = payload['parent_id']
    file_stream = BytesIO(raw_file)
    response = Drive.uploadFile(file_name, mime_type, file_stream, parent_id)
    return response

@bp.route('/upload_csv_files', methods=['POST'])
def upload_csv_files_route():
    payload = request.get_json(force=True)
    files = payload['files']
    parent_id = payload['parent_id']
    response = Drive.uploadCSVFiles(files, parent_id)
    return response

