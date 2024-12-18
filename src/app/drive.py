from flask import Blueprint, request, send_file
from io import BytesIO
from src.components.drive import GoogleDrive
from src.utils.response import Response

bp = Blueprint('drive', __name__)

Drive = GoogleDrive()

@bp.route('/get_shared_drive_info', methods=['POST'])
def get_shared_drive_info_route():
    payload = request.get_json(force=True)
    drive_name = payload['drive_name']
    return Drive.get_shared_drive_info(drive_name)

@bp.route('/get_folder_info', methods=['POST'])
def get_folder_info_route():
    payload = request.get_json(force=True)
    parent_id = payload['parent_id']
    folder_name = payload['folder_name']
    return Drive.get_folder_info(parent_id, folder_name)

@bp.route('/get_files_in_folder', methods=['POST'])
def get_files_in_folder_route():
    payload = request.get_json(force=True)
    parent_id = payload['parent_id']
    return Drive.get_files_in_folder(parent_id)

@bp.route('/get_file_info', methods=['POST'])
def get_file_info_route():
    payload = request.get_json(force=True)
    parent_id = payload['parent_id']
    file_name = payload['file_name']
    return Drive.get_file_info(parent_id, file_name)

@bp.route('/get_file_info_by_id', methods=['POST'])
def get_file_info_by_id_route():
    payload = request.get_json(force=True)
    file_id = payload['file_id']
    return Drive.get_file_info_by_id(file_id)

@bp.route('/reset_folder', methods=['POST'])
def reset_folder_route():
    payload = request.get_json(force=True)
    folder_id = payload['folder_id']
    return Drive.reset_folder(folder_id)

@bp.route('/delete_file', methods=['POST'])
def delete_file_route():
    payload = request.get_json(force=True)
    fileId = payload['fileId']
    return Drive.delete_file(fileId)

@bp.route('/move_file', methods=['POST'])
def move_file_route():
    payload = request.get_json(force=True)
    f = payload['file']
    new_parent_id = payload['new_parent_id']
    return Drive.move_file(f, new_parent_id)

@bp.route('/download_file', methods=['POST'])
def download_file_route():
    payload = request.get_json(force=True)
    response = Drive.download_file(payload['file_id'])
    try:
        f = BytesIO(response['content'])
        return send_file(f, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        return Response.error(f"Error downloading file: {str(e)}")

@bp.route('/rename_file', methods=['POST'])
def rename_file_route():
    payload = request.get_json(force=True)
    fileId = payload['fileId']
    newName = payload['newName']
    return Drive.rename_file(fileId, newName)

@bp.route('/upload_file', methods=['POST'])
def upload_file_route():
    payload = request.get_json(force=True)
    f = payload['file']
    fileName = payload['fileName']
    mimeType = payload['mimeType']
    parentFolderId = payload['parentFolderId']
    return Drive.upload_file(fileName, mimeType, f, parentFolderId)
