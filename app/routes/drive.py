from flask import Blueprint, request
from app.helpers.google import GoogleDrive

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

@bp.route('/upload_csv_files', methods=['POST'])
def upload_csv_files_route():
    payload = request.get_json(force=True)
    files = payload['files']
    parent_id = payload['parent_id']
    response = Drive.uploadCSVFiles(files, parent_id)
    return response

@bp.route('/get_files_in_folder', methods=['POST'])
def get_files_in_folder_route():
    payload = request.get_json(force=True)
    parent_id = payload['parent_id']
    response = Drive.getFilesInFolder(parent_id)
    return response

@bp.route('/move_file', methods=['POST'])
def move_file_route():
    payload = request.get_json(force=True)
    file_id = payload['file_id']
    new_parent_id = payload['new_parent_id']
    response = Drive.moveFile(file_id, new_parent_id)
    return response

@bp.route('/rename_files', methods=['POST'])
def rename_files_route():
    payload = request.get_json(force=True)
    files = payload['files']
    response = Drive.renameFiles(files)
    return response