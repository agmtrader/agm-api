from flask import Blueprint, request, send_file
from io import BytesIO
from src.components.drive import GoogleDrive
from src.utils.response import Response

bp = Blueprint('drive', __name__)

Drive = GoogleDrive()

@bp.route('/user-info', methods=['GET'])
def get_user_info_route():
    return Drive.get_user_info()

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
    file_id = payload['file_id']
    return Drive.delete_file(file_id)

@bp.route('/move_file', methods=['POST'])
def move_file_route():
    payload = request.get_json(force=True)
    f = payload['file']
    new_parent_id = payload['new_parent_id']
    return Drive.move_file(f, new_parent_id)

@bp.route('/rename_file', methods=['POST'])
def rename_file_route():
    payload = request.get_json(force=True)
    file_id = payload['file_id']
    new_name = payload['new_name']
    return Drive.rename_file(file_id, new_name)

@bp.route('/upload_file', methods=['POST'])
def upload_file_route():
    payload = request.get_json(force=True)
    f = payload['file']
    file_name = payload['file_name']
    mime_type = payload['mime_type']
    parent_folder_id = payload['parent_folder_id']
    return Drive.upload_file(file_name, mime_type, f, parent_folder_id)

@bp.route('/download_file', methods=['POST'])
def download_file_route():
    payload = request.get_json(force=True)
    file_id = payload['file_id']
    parse = payload['parse']
    response = Drive.download_file(file_id, parse)
    try:
        if parse:
            return Response.success(response['content'])
        else:
            f = BytesIO(response['content'])
            return send_file(f, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        return Response.error(f"Error downloading file: {str(e)}")

@bp.route('/export_file', methods=['POST'])
def export_file_route():
    payload = request.get_json(force=True)
    file_id = payload['file_id']
    mime_type = payload['mime_type']
    parse = payload['parse']
    response = Drive.export_file(file_id, mime_type, parse)
    try:
        if parse:
            return Response.success(response['content'])
        else:
            f = BytesIO(response['content'])
            return send_file(f, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        return Response.error(f"Error exporting file: {str(e)}")