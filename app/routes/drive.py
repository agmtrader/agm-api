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

@bp.route('/get_file_info', methods=['POST'])
def get_file_info_route():
    payload = request.get_json(force=True)
    parent_id = payload['parent_id']
    file_name = payload['file_name']
    response = Drive.getFileInfo(parent_id, file_name)
    return response

@bp.route('/get_file_info_by_id', methods=['POST'])
def get_file_info_by_id_route():
    payload = request.get_json(force=True)
    file_id = payload['file_id']
    response = Drive.getFileInfoById(file_id)
    return response

@bp.route('/reset_folder', methods=['POST'])
def reset_folder_route():
    payload = request.get_json(force=True)
    folder_id = payload['folder_id']
    response = Drive.resetFolder(folder_id)
    return response

@bp.route('/delete_file', methods=['POST'])
def delete_file_route():
    payload = request.get_json(force=True)
    fileId = payload['fileId']
    response = Drive.deleteFile(fileId)
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
    return send_file(f, mimetype=payload['mime_type'])

@bp.route('/export_file', methods=['POST'])
def export_file_route():
    payload = request.get_json(force=True)
    response = Drive.exportFile(payload['file_id'], payload['mime_type'])
    f = BytesIO(response['content'])
    return send_file(f, mimetype=payload['mime_type'])

@bp.route('/rename_file', methods=['POST'])
def rename_file_route():
    payload = request.get_json(force=True)
    fileId = payload['fileId']
    newName = payload['newName']
    response = Drive.renameFile(fileId, newName)
    return response

@bp.route('/upload_file', methods=['POST'])
def upload_file_route():
    payload = request.get_json(force=True)
    f = payload['file']
    fileName = payload['fileName']
    mimeType = payload['mimeType']
    parentFolderId = payload['parentFolderId']
    response = Drive.uploadFile(fileName, mimeType, f, parentFolderId)
    return response