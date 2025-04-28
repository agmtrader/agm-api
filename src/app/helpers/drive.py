
from flask import Blueprint, request
from src.helpers.drive import GoogleDrive
from src.utils.scope_manager import verify_scope

bp = Blueprint('drive', __name__)
drive = GoogleDrive()

@bp.route('/download_file', methods=['POST'])
@verify_scope('drive/download_file')
def download_file_route():
    payload = request.get_json(force=True)
    file_id = payload['file_id']
    return drive.download_file(file_id=file_id)

@bp.route('/upload_file', methods=['POST'])
@verify_scope('drive/upload_file')
def upload_file_route():
    payload = request.get_json(force=True)
    file_id = payload['file_id']
    return drive.upload_file(file_id=file_id)